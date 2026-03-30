INSERT INTO sources (source_code, source_name, source_family, schedule_type, metadata_json)
VALUES
  ('filmyard', 'Filmyard', 'boxoffice_daily', 'daily', '{"priority": 1}'),
  ('elcinema', 'elCinema', 'boxoffice_weekly', 'weekly', '{"priority": 2}'),
  ('bom', 'Box Office Mojo', 'market_boxoffice', 'weekly', '{"priority": 3}'),
  ('imdb', 'IMDb', 'ratings_popularity', 'daily', '{"priority": 4}')
ON CONFLICT (source_code) DO NOTHING;

