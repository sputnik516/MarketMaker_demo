Stock Market simulation

This is a simulation of a stock market. There are many participants (traders) sending their buy and sell offers. We
will run a trading strategy that analyses these offers and decides which to trade on.
Prices follow the "market" price but have enough variation to simulate actual market mechanics. Our strategy will
enter a position when it sees an attractive risk / reward, attempt to exit it at a profit, and maintain it's size within
 preset risk constraints. If it is carrying too much risk on the long / short side, it will begin unwinding the position.

Requires:

python 3.6+
pandas
KDB
RabbitMQ

To run:

Start KDB server: q -p 5000

In separate terminals, run:
    server.py
    client.py

Run several instances of client.py to simulate multiple market participants.

server.py will consolidate order from various clients, and our "strategy" will trade on those orders when it's targets
are met. It will maintain it's own portfolio allocation and risk control, and attempt to generate a posititive PnL. 
RabbitMQ will consolidate all orders coming from the market participants into one order book, demonstrating it's ability to process aynchronous messages from many sources.

Please see below for all of this in action:

Youtube demo: https://www.youtube.com/watch?v=pMI73qY8mTk
