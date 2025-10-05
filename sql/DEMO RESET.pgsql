-- DEMO RESET

-- wipe rows
DELETE FROM ledger_entries;
DELETE FROM trades;
DELETE FROM positions;
DELETE FROM users;

-- seed users
INSERT INTO users (id, cash_balance) VALUES
  ('u1', 4000),
  ('u2', 2000),
  ('u3',   50),
  ('mm', 10000000);

-- seed stock positions

-- u1: none

-- u2: 1/2/3 pattern across top 10
INSERT INTO positions (user_id, symbol, qty) VALUES
  ('u2','AAPL', 1),
  ('u2','MSFT', 2),
  ('u2','GOOG', 3),
  ('u2','AMZN', 1),
  ('u2','META', 2),
  ('u2','NVDA', 3),
  ('u2','TSLA', 1),
  ('u2','NFLX', 2),
  ('u2','AVGO', 3),
  ('u2','AMD',  1);

-- u3: lots of each
INSERT INTO positions (user_id, symbol, qty) VALUES
  ('u3','AAPL', 100),
  ('u3','MSFT', 100),
  ('u3','GOOG', 100),
  ('u3','AMZN', 100),
  ('u3','META', 100),
  ('u3','NVDA', 100),
  ('u3','TSLA', 100),
  ('u3','NFLX', 100),
  ('u3','AVGO', 100),
  ('u3','AMD',  100);

-- mm: massive inventory (functionally infinite)
INSERT INTO positions (user_id, symbol, qty) VALUES
  ('mm','AAPL', 1000000),
  ('mm','MSFT', 1000000),
  ('mm','GOOG', 1000000),
  ('mm','AMZN', 1000000),
  ('mm','META', 1000000),
  ('mm','NVDA', 1000000),
  ('mm','TSLA', 1000000),
  ('mm','NFLX', 1000000),
  ('mm','AVGO', 1000000),
  ('mm','AMD',  1000000);

-- sanity checks
SELECT * FROM positions ORDER BY user_id, symbol;
SELECT * FROM users ORDER BY id;