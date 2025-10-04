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

-- u2: 1/2/3 
INSERT INTO positions (user_id, symbol, qty) VALUES
  ('u2','AAPL', 1),
  ('u2','MSFT', 2),
  ('u2','GOOG', 3);

-- u3: lots
INSERT INTO positions (user_id, symbol, qty) VALUES
  ('u3','AAPL', 100),
  ('u3','MSFT', 100),
  ('u3','GOOG', 100);

-- mm: massive inventory (functionally infinite)
INSERT INTO positions (user_id, symbol, qty) VALUES
  ('mm','AAPL', 1000000),
  ('mm','MSFT', 1000000),
  ('mm','GOOG', 1000000);

SELECT * FROM "positions" LIMIT 1000;

SELECT * FROM "users" LIMIT 1000;
