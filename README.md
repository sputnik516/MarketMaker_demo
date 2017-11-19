Stock Market simulation

This is a simulation of a market-maker algorithm. Prices are simulated assuming the Random Walk Theory. The algorithm
uses historical data to decide prices and sizes of offers to send to the market. It also maintains a simulated order book,
selecting best bids and offers. Finally, it adjusts for expected market impact.

Once offers are taken, the algorithm maintains a position within risk parameters, and attempts to liquidate it at a
profit.

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
are met. It will maintain it's own portfolio allocation and risk control, and attempt to generate a posititive PnL

Please see below for all of this in action:

Youtube demo: https://www.youtube.com/watch?v=pMI73qY8mTk