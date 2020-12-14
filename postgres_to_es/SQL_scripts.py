CREATE_TMP_LAST_UPDATED_FILMS = '''
            CREATE TEMPORARY TABLE tmp_last_updated_films(
                film_id UUID,
                changed_parameter VARCHAR,
                last_update TIMESTAMP WITH TIME ZONE,
                UNIQUE (film_id, last_update)
            );
            '''
FIND_UPDATED_GENRES = '''
            INSERT INTO tmp_last_updated_films (film_id, changed_parameter, last_update)
            SELECT fw.id, 'change genre', g.updated_at FROM content.film fw
                INNER JOIN content.film_genre gfw ON fw.id = gfw.film_id
                INNER JOIN content.genre g ON gfw.genre_id = g.id
            WHERE g.updated_at > %s
            ON CONFLICT (film_id, last_update) DO NOTHING;
            '''
FIND_UPDATED_PERSONS = '''
            INSERT INTO tmp_last_updated_films (film_id, changed_parameter, last_update)
            SELECT fw.id, 'change person', p.updated_at FROM content.film fw
                INNER JOIN content.film_person pfw ON fw.id = pfw.film_id
                INNER JOIN content.person p ON pfw.person_id = p.id
            WHERE p.updated_at > %s
            ON CONFLICT (film_id, last_update) DO NOTHING;
            '''
FIND_UPDATED_FILMS = '''
            INSERT INTO tmp_last_updated_films (film_id, changed_parameter, last_update)
            SELECT fw.id, 'change film', fw.updated_at FROM content.film fw
            WHERE fw.updated_at > %s
            ON CONFLICT (film_id, last_update) DO NOTHING;
            '''

CREATE_TMP_FILM_GENRES = '''
            CREATE TEMPORARY TABLE tmp_film_genres as
            SELECT tuf.film_id as film_id, string_agg(g.name, ',') as genre
            FROM tmp_last_updated_films tuf
                INNER JOIN content.film_genre gfw ON tuf.film_id = gfw.film_id
                INNER JOIN content.genre g ON gfw.genre_id = g.id
                GROUP BY tuf.film_id, tuf.last_update
            ORDER BY tuf.last_update
            FETCH FIRST %s ROWS WITH TIES;
            '''

CREATE_TMP_FILM_PERSONS = '''
            CREATE TEMPORARY TABLE tmp_film_persons as
            SELECT
                fw.id as film_id,
                fw.rating as rating,
                fw.title as title,
                fw.description as description,
                '[' || string_agg('{"id":"' || cast(p.id as varchar) || '", "name":"'
                || p.full_name || '", "role":"' || pfw.role || '"}', ',') || ']' as jsonify_persons
            FROM tmp_last_updated_films tuf
                INNER JOIN content.film fw ON tuf.film_id = fw.id
                INNER JOIN content.film_person pfw ON fw.id = pfw.film_id
                INNER JOIN content.person p ON pfw.person_id = p.id
            GROUP BY tuf.last_update, fw.rating, fw.title, fw.description, fw.id
            ORDER BY tuf.last_update
            FETCH FIRST %s ROWS WITH TIES;
            '''

CLEAR_DATA_OVER_LIMIT = '''
            DELETE FROM tmp_last_updated_films tuf
            WHERE tuf.film_id NOT IN (SELECT tuf2.film_id 
                                      FROM tmp_last_updated_films tuf2 
                                      ORDER BY tuf2.LAST_UPDATE 
                                      FETCH FIRST %s ROWS WITH TIES);
            '''

GET_UPDATED_FILMS_INFO = '''
            SELECT
                tuf.film_id as film_id,
                fw.title as title,
                fw.description as description,
                tfp.rating as rating,
                tfg.genre as genre,
                tfp.jsonify_persons as jsonify_persons,
                tuf.last_update as last_update
            FROM tmp_last_updated_films tuf
                INNER JOIN content.film fw ON tuf.film_id = fw.id
                INNER JOIN tmp_film_genres tfg ON tuf.film_id = tfg.film_id
                INNER JOIN tmp_film_persons tfp ON tfg.film_id = tfp.film_id
            ORDER BY tuf.last_update;
            '''

GET_UPDATED_GENRES_INFO = '''
            SELECT 
                g.id as genre_id,
                g.name as genre_name, 
                g.description as genre_description
            FROM tmp_last_updated_films tuf
                INNER JOIN content.film_genre gfw ON tuf.film_id = gfw.film_id
                INNER JOIN content.genre g ON gfw.genre_id = g.id
            WHERE tuf.changed_parameter = 'change genre'
            GROUP BY g.id;
            '''

GET_UPDATED_PERSONS_INFO = '''
            SELECT 
                p.id as person_id,
                p.full_name as full_name, 
                fp.role as role, 
                string_agg(DISTINCT (cast(tuf.film_id as VARCHAR)), ', ') as person_films
            FROM tmp_last_updated_films tuf
                INNER JOIN content.film_person fp ON tuf.film_id = fp.film_id
                INNER JOIN content.person p ON fp.person_id = p.id
            WHERE tuf.changed_parameter = 'change person'
            GROUP BY p.id, p.full_name, fp.role
            ORDER BY p.full_name;
            '''

DROP_TMP_TABLES = '''
            DROP TABLE tmp_film_persons;
            DROP TABLE tmp_film_genres;
            DROP TABLE tmp_last_updated_films;
            '''
