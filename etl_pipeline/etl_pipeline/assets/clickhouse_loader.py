import json
from dagster import asset, Output, MetadataValue
import pandas as pd
from etl_pipeline.config.clickhouseSchemas import *
from etl_pipeline.config.settings import MINIO_BUCKET


@asset(required_resource_keys={"minio_client", "clickhouse_client"})
def load_minio_to_clickhouse(context):
    """
    Transfert direct des données StatsBomb du MinIO vers ClickHouse.
    """
    minio = context.resources.minio_client
    ch = context.resources.clickhouse_client
    init_clickhouse_schema(ch)
    objects = minio.list_objects(MINIO_BUCKET, recursive=True)

    stats = {'total': 0, 'loaded': 0, 'skipped': 0, 'failed': 0, 'rows_loaded': 0, 'files': []}

    for obj in objects:
        file_path = obj.object_name
        stats['total'] += 1
        try:
            if is_file_loaded(ch, file_path):
                stats['skipped'] += 1
                stats['files'].append({'file': file_path, 'status': 'skipped'})
                continue

            data = minio.get_object(MINIO_BUCKET, file_path).read()
            json_data = json.loads(data.decode('utf-8'))
            table_name = get_target_table(file_path)

            if not table_name:
                stats['skipped'] += 1
                stats['files'].append({'file': file_path, 'status': 'skipped', 'reason': 'type non géré'})
                continue

            validate_data(table_name, json_data)
            row_count = load_data_to_table(ch, table_name, json_data, file_path)
            record_loaded_file(ch, file_path)

            stats['loaded'] += 1
            stats['rows_loaded'] += row_count
            stats['files'].append({'file': file_path, 'status': 'loaded', 'table': table_name, 'rows': row_count})

        except Exception as e:
            stats['failed'] += 1
            stats['files'].append({'file': file_path, 'status': 'failed', 'error': str(e)})
            context.log.error(f"Erreur sur {file_path} : {str(e)}")

    summary = generate_summary(stats)
    return Output(None, metadata={
        "summary": MetadataValue.md(summary),
        "stats": stats,
        "preview": MetadataValue.md("✅ Chargement complet terminé")
    })

def init_clickhouse_schema(ch):
    schemas = [
        """CREATE TABLE IF NOT EXISTS load_history (
            file_path String,
            load_time DateTime DEFAULT now()
        ) ENGINE = MergeTree() ORDER BY (load_time)""",
        EVENTS_SCHEMA,
        MATCHES_SCHEMA,
        LINEUPS_SCHEMA,
        THREE_SIXTY_SCHEMA,
        COMPETITIONS_SCHEMA
    ]
    
    for i, schema in enumerate(schemas):
        try:
            print(f"Executing schema {i+1} of {len(schemas)}")
            ch.execute(schema)
            print(f"Schema {i+1} executed successfully")
        except Exception as e:
            print(f"Error executing schema {i+1}:")
            print(schema)
            print(f"Error details: {str(e)}")
            raise

def is_file_loaded(ch, file_path):
    return ch.execute("SELECT 1 FROM load_history WHERE file_path = %(path)s LIMIT 1", {'path': file_path})

def get_target_table(file_path):
    if 'data/events/' in file_path:
        return 'events'
    elif 'data/matches/' in file_path:
        return 'matches'
    elif 'data/lineups/' in file_path:
        return 'lineups'
    elif 'data/three-sixty/' in file_path:
        return 'three_sixty'
    elif file_path == 'data/competitions.json':
        return 'competitions'
    return None

def validate_data(table_name, data):
    if not data:
        raise ValueError(f"Données vides pour {table_name}")
    if not isinstance(data, (list, dict)):
        raise ValueError(f"Format de données invalide pour {table_name}")
    if table_name == 'events':
        required_fields = ['id', 'match_id', 'period', 'timestamp', 'type']
        for e in (data if isinstance(data, list) else [data]):
            if not all(field in e for field in required_fields):
                missing = [f for f in required_fields if f not in e]
                raise ValueError(f"Champs manquants dans event: {missing}")

def load_data_to_table(ch, table_name, data, file_path):
    loader = {
        'events': load_events,
        'matches': load_matches,
        'lineups': load_lineups,
        'three_sixty': load_three_sixty,
        'competitions': load_competitions
    }.get(table_name)
    if not loader:
        raise ValueError(f"Chargeur inconnu pour {table_name}")
    return loader(ch, data, file_path)

def record_loaded_file(ch, path):
    ch.execute("INSERT INTO load_history (file_path) VALUES", [{'file_path': path}])

def generate_summary(stats):
    df = pd.DataFrame(stats['files'])
    if not df.empty:
        table_stats = df[df['status'] == 'loaded'].groupby('table').agg({'file': 'count', 'rows': 'sum'}).to_markdown()
    else:
        table_stats = "Aucun fichier chargé"
    return (
        "## Rapport de Chargement Football\n\n"
        f"### Statistiques Globales\n"
        f"- Fichiers traités : {stats['total']}\n"
        f"- Chargés avec succès : {stats['loaded']}\n"
        f"- Lignes chargées : {stats['rows_loaded']}\n"
        f"- Ignorés (déjà traités) : {stats['skipped']}\n"
        f"- Échecs : {stats['failed']}\n\n"
        f"### Répartition par Table\n{table_stats}\n\n"
        f"### Détails Complets\n{df.to_markdown()}"
    )
