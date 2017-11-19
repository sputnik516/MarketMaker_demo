import pandas as pd
import numpy as np
import json
import random
from scipy import stats, special
from kdb import PublisherThread


class MM(object):
    def __init__(self, max_pos=1000000, max_trade_value=200000):
        self.max_pos = max_pos
        self.max_trade_value = max_trade_value
        self.fill_rate = 0.25
        self.px_data = pd.DataFrame(columns=['timestamp', 'Trade Price', 'Bid', 'Ask'])
        self.lookback_periods = 25

        # Weight of historical data diminishes with time
        self.weights = list(map(lambda r: 100 * pow(1 - 0.5, r), range(0, self.lookback_periods)))
        self.portfoolio = {'Shares': 0, 'Cost': 0, 'PnL': 0, 'Avg_Cost': 0, 'Unrealized_PnL': 0}

        # Connection to kdb
        self.pub_thread = PublisherThread()

    def calc_b_a(self, data):
        """Calculate and optimize bid / ask size, price"""
        # Calculate our expected bid / ask
        mkt_bid = data['Bid'].values[-1]
        mkt_ask = data['Ask'].values[-1]
        last_trade = data['Trade Price'].values[-1]
        shares = self.calc_shares(data=data, last_trade=last_trade)

        k = (mkt_bid - mkt_ask) / (mkt_bid + mkt_ask) * -100

        our_bid = np.average(data['Bid'], weights=self.weights) - k
        our_ask = np.average(data['Ask'], weights=self.weights) + k

        self.portfoolio['Unrealized_PnL'] = self.portfoolio['Shares'] * last_trade - self.portfoolio['Avg_Cost'] * self.portfoolio['Shares']

        if shares == 0:
            # Skip 0 share orders
            pass
        elif abs(self.portfoolio['Cost']) >= self.max_pos * .75:
            # If position size at or above 95% of max, reduce position
            self.risk_control(bid=mkt_bid, ask=mkt_ask, last_trade=last_trade)

        elif our_bid >= mkt_bid:
            # Buy at bid
            self.trade(shares=shares, price=mkt_bid, last_trade=last_trade)
        elif our_ask <= mkt_ask:
            # Sell at ask
            self.trade(shares=-shares, price=mkt_ask, last_trade=last_trade)
        else:
            print('No order placed')

    def risk_control(self, bid, ask, last_trade):
        """Optimize position size to match risk targets.
        If position size at or above 95% of max, reduce position"""
        # Are we long or short?
        pos_cost = abs(self.portfoolio['Cost'])
        side = self.sign(self.portfoolio['Cost'])

        if side > 0:
            # Sell if long
            price = bid
        else:
            # Cover if short
            price = ask

        shares = (pos_cost * 0.05) / last_trade * side * -1
        shares = int(round(shares, -2))
        print('Reducing position, {} shares, price: {}'.format(shares, price))
        self.trade(shares, price, last_trade)

    def calc_shares(self, data, last_trade):
        """Calculate share quantity"""
        shares = special.ndtr(stats.zscore(np.array(data['Pct_change']))[-1])
        shares *= self.max_trade_value / last_trade

        # Adjust for 100 share lots
        shares = int(round(shares, -2))
        return shares

    def sign(self, x):
        if x > 0:
            return 1
        elif x == 0:
            return 0
        else:
            return -1

    def trade(self, shares, price, last_trade):
        """Place a trade, simulating likelihood of trade being filled"""
        # Simulate likelyhood of fill
        likely_fill = random.randint(0, 100) / 100
        if likely_fill > self.fill_rate:
            print('Order not filled')
            return

        trade_value = shares * price

        # Net long / short
        if self.sign(shares) != self.sign(self.portfoolio['Shares']) and self.portfoolio['Shares'] != 0:
            trade_pnl = (shares * self.portfoolio['Avg_Cost']) - trade_value

            self.portfoolio['PnL'] += trade_pnl

        # Flat
        else:
            trade_pnl = 0

        self.portfoolio['Shares'] += shares
        self.portfoolio['Cost'] += shares * price + trade_pnl
        self.portfoolio['Avg_Cost'] = self.portfoolio['Cost'] / self.portfoolio['Shares']
        self.portfoolio['Unrealized_PnL'] = self.portfoolio['Shares'] * last_trade - self.portfoolio['Avg_Cost'] * self.portfoolio['Shares']

        print('Pnl booked: {} | Portfolio: {}'.format(trade_pnl, self.portfoolio))
        print(last_trade)

    def run_strat(self, context):
        """
        Run strategy continuously
        inputs: context, received from RabbitMQ queue. Contains bids, asks, timestamps of offers

        Aggregates them into order book (self.px_data)
        """
        # Get new offers from market orders
        context = json.loads(context.decode('UTF-8'))
        new_offer = pd.DataFrame.from_dict(context, orient='index').transpose()
        new_offer['timestamp'] = pd.to_datetime(new_offer['timestamp'])

        # Save to kbd
        self.pub_thread.run(data=new_offer)

        # Aggregate orders
        self.px_data = self.px_data.append(new_offer)
        self.px_data.reset_index(inplace=True, drop=True)

        # Add percent change column
        self.px_data['Pct_change'] = self.px_data['Trade Price'].pct_change()
        self.px_data['Pct_change'].fillna(0, inplace=True)

        # Fill missing data with previous value
        self.px_data.fillna(method='ffill', inplace=True)

        if len(self.px_data) > self.lookback_periods:

            self.px_data = self.px_data.iloc[1:]
            self.px_data.reset_index(inplace=True, drop=True)
            self.calc_b_a(data=self.px_data)

        else:
            print('Not enough historical trade data yet...')


if __name__ == "__main__":
    m = MM()
    m.run_strat()
    print('<Done>')



