-- Optional small seed data to test permissions and queries
INSERT INTO land.raw_pokies (machine_id, lga, report_date, turnover, profit, operator_name, source, raw_payload)
VALUES
('M-100','Sydney', '2024-01-01', 1000, 200, 'Operator A', 'sample', '{"note":"sample row 1"}'),
('M-101','Penrith', '2024-01-02', 1200, 250, 'Operator B', 'sample', '{"note":"sample row 2"}');

INSERT INTO stage.pokies_stage (machine_id, lga, report_date, turnover, profit, operator_name, transform_meta)
VALUES
('M-100','Sydney','2024-01-01',1000,200,'Operator A','{"step":"sample transform"}');