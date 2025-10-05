# Dragonfly

Developed by: Peyton Ratzer
Coding Assisted by ChatGPT

This is a simple stock trading service demo created as a Distributed Computing assignment. This assignment was created by Peyton Ratzer with coding, syntax, and organizational help from ChatGPT. The following file is instructions on how to run the demo in a visual studio environment.

Firstly, ensure you have all applicable modules installed in the VSCode environement including docker and postgreSQL modules. Set up 5 command lines for 5 different purposes and establish them as in the tutorial video.

Use the Command
docker compose up -d

With the docker compose file to establish the RabbitMQ and PostgreSQL containers and start the three python workers in three seperate command lines with the following commands as in the video.

python services/risk/worker.py
python services/matching/worker.py
python services/settlement/worker.py

Use the SQL script called DEMO RESET to load initial data into the PostgreSQL database to be interacted with.
You can use the following command to verify if your SQL data has been loaded.

(Invoke-RestMethod "http://127.0.0.1:8000/balances/all").users |
  Select-Object @{n='user';e={$_.user_id}},
                @{n='cash';e={$_.cash}},
                @{n='positions';e={ ($_.positions | ForEach-Object { "$($_.symbol):$($_.qty)" }) -join ', ' }} |
  Format-Table -AutoSize

Before invoking the OrdersAPI, you must initialize the messages in RabbitMQ using uvicorn which needs to be installed into your python environment. This should be done in a 4th command line seperate from each python worker.

python -m uvicorn services.orders_api.main:app --reload
This is the command to establish the orders api.

Now the environment should be configured and you can run commands in the form;

Invoke-RestMethod -Method POST "http://127.0.0.1:8000/orders?user_id=u1&side=BUY&qty=2&symbol=MSFT"

Where you can alter user_id, side, quantity, and symbol and make changes in the database. The orders should be visible on the RabbitMQ container as shown in the video. Ensure to run the commands in a seperate 5th command line.


Demo Video https://youtu.be/jmQI1fsKoZY
