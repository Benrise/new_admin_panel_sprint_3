schema = 'content'

sql_extract_updated_film_work_records_query = """
    SELECT
        film_work.id,
        film_work.title,
        film_work.description ,
        film_work.rating,
        film_work.type,
        film_work.created_at,
        film_work.updated_at,
        COALESCE (
            json_agg(
                DISTINCT jsonb_build_object(
                    'person_role', pfw.role,
                    'person_id', person.id,
                    'person_name', person.full_name
                )
            ) FILTER (WHERE person.id is not null),
            '[]'
        ) as persons,
        array_agg(DISTINCT genre.name) as genres
    FROM content.film_work
        LEFT JOIN content.person_film_work pfw ON pfw.film_work_id = film_work.id
        LEFT JOIN content.person ON person.id = pfw.person_id
        LEFT JOIN content.genre_film_work gfw ON gfw.film_work_id = film_work.id
        LEFT JOIN content.genre  ON genre.id = gfw.genre_id
    WHERE %(table)s.id in %(pkeys)s
    AND film_work.id::text > %(last_id)s
    GROUP BY film_work.id
    ORDER BY film_work.id
    LIMIT %(limit)s;
"""

sql_extract_last_updated_table_query = """
    SELECT id, updated_at FROM %(table)s
    WHERE updated_at > %(updated_at)s
    ORDER BY updated_at
    LIMIT %(limit)s
"""