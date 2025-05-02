import hashlib
import requests
import io
import pandas as pd
from dagster import asset, Output, MetadataValue
from etl_pipeline.config.settings import MINIO_BUCKET, TARGET_PATHS


@asset(required_resource_keys={"minio_client"})
def transfer_statsbomb_to_minio(context):
    """
    Transfert direct des données StatsBomb vers MinIO sans traitement intermédiaire.
    Conserve la structure originale des fichiers.
    """
    REPO_URL = "https://api.github.com/repos/statsbomb/open-data/git/trees/master?recursive=1"
    RAW_BASE_URL = "https://raw.githubusercontent.com/statsbomb/open-data/master/"

    minio_client = context.resources.minio_client
    
    # Récupération de l'arborescence
    response = requests.get(REPO_URL)
    tree = response.json().get("tree", [])
    
    # Filtrage des fichiers cibles
    json_files = [
        f["path"] for f in tree 
        if f["path"].endswith(".json") and 
        any(f["path"].startswith(path) for path in TARGET_PATHS)
    ]

    stats = {
        'total_files': len(json_files),
        'uploaded': 0,
        'skipped': 0,
        'failed': 0,
        'processed_files': []
    }

    for idx, file_path in enumerate(json_files):
        try:
            # Progress tracking
            progress = (idx + 1) / stats['total_files'] * 100
            context.log.info(
                f"Traitement du fichier {idx + 1}/{stats['total_files']} "
                f"({progress:.1f}%) : {file_path}"
            )
            
            # Téléchargement par chunks
            url = RAW_BASE_URL + file_path
            r = requests.get(url, stream=True)
            r.raise_for_status()
            
            # Calcul du hash pendant le stream
            hash_sha256 = hashlib.sha256()
            content = bytearray()
            for chunk in r.iter_content(chunk_size=8192):
                content.extend(chunk)
                hash_sha256.update(chunk)
            
            file_hash = hash_sha256.hexdigest()
            object_name = f"{file_path}"  # Conserve le chemin original
            
            # Vérification des doublons
            try:
                existing = minio_client.stat_object(MINIO_BUCKET, object_name)
                if existing.metadata.get('x-amz-meta-sha256') == file_hash:
                    stats['skipped'] += 1
                    stats['processed_files'].append({
                        'file': file_path,
                        'status': 'skipped',
                        'size': len(content)
                    })
                    continue
            except Exception:
                pass
            
            # Upload vers MinIO
            minio_client.put_object(
                MINIO_BUCKET,
                object_name,
                data=io.BytesIO(content),
                length=len(content),
                content_type="application/json",
                metadata={'sha256': file_hash}
            )
            
            stats['uploaded'] += 1
            stats['processed_files'].append({
                'file': file_path,
                'status': 'uploaded',
                'size': len(content)
            })

        except Exception as e:
            stats['failed'] += 1
            stats['processed_files'].append({
                'file': file_path,
                'status': 'failed',
                'error': str(e)
            })
            context.log.error(f"Échec sur {file_path}: {str(e)}")

    # Génération d'un rapport de synthèse
    summary = (
        "## Rapport de transfert StatsBomb → MinIO\n\n"
        f"- **Fichiers traités:** {stats['total_files']}\n"
        f"- **Nouveaux fichiers uploadés:** {stats['uploaded']}\n"
        f"- **Fichiers inchangés (skipped):** {stats['skipped']}\n"
        f"- **Échecs:** {stats['failed']}\n\n"
        "### Détails par fichier\n"
        f"{pd.DataFrame(stats['processed_files']).to_markdown()}"
    )

    return Output(
        None,  # On ne retourne pas les données
        metadata={
            "summary": MetadataValue.md(summary),
            "stats": stats,
            "preview": MetadataValue.md("✅ Transfert terminé - Voir le summary pour les détails")
        }
    )
