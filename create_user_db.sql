CREATE USER :user WITH LOGIN PASSWORD ':passwd';
CREATE DATABASE :user;
GRANT ALL ON DATABASE :user TO :user;