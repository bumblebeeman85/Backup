"""
Celery tasks for asynchronous backup processing and search indexing.
"""
import os
import json
from typing import Dict, Any, List, Optional
from celery import Celery
from celery.exceptions import Retry
from celery.utils.log import get_task_logger

from . import main, db
from .search import MeilisearchClient

logger = get_task_logger(__name__)

# Configure Celery
redis_url = os.getenv('REDIS_URL', 'redis://localhost:6379/0')
celery_app = Celery(
    'm365_backup',
    broker=redis_url,
    backend=redis_url,
    include=['m365_backup.tasks']
)

# Configure Celery settings
celery_app.conf.update(
    task_serializer='json',
    accept_content=['json'],
    result_serializer='json',
    timezone='UTC',
    enable_utc=True,
    task_track_started=True,
    task_routes={
        'm365_backup.tasks.backup_tenant_async': {'queue': 'backup'},
        'm365_backup.tasks.backup_all_tenants_async': {'queue': 'backup'},
        'm365_backup.tasks.index_messages_async': {'queue': 'search'},
    },
    worker_prefetch_multiplier=1,
    task_acks_late=True,
    worker_max_tasks_per_child=50,
)


@celery_app.task(bind=True, autoretry_for=(Exception,), retry_kwargs={'max_retries': 3, 'countdown': 60})
def backup_tenant_async(self, tenant_data: Dict[str, Any], options: Dict[str, Any]) -> Dict[str, Any]:
    """
    Asynchronously backup a single tenant.
    
    Args:
        tenant_data: Tenant configuration (name, tenant_id, client_id, client_secret)
        options: Backup options (mails_per_user, download_attachments, label)
    
    Returns:
        Dict containing backup results and snapshot information
    """
    try:
        logger.info(f"Starting backup for tenant: {tenant_data['name']}")
        
        # Update task state to show progress
        self.update_state(
            state='PROGRESS',
            meta={'stage': 'authenticating', 'progress': 10, 'message': 'Authenticating with Microsoft Graph...'}
        )
        
        # Perform the backup using existing logic
        collected_messages = main.backup_tenant(tenant_data, options)
        
        self.update_state(
            state='PROGRESS',
            meta={'stage': 'storing', 'progress': 80, 'message': 'Storing messages in database...'}
        )
        
        if collected_messages:
            # Store snapshot
            _db = db.DatabaseOperations()
            _db.init_db()
            
            snapshot_label = options.get('label', f"async-{tenant_data['name']}")
            snapshot_id, inserted_count = _db.store_snapshot(snapshot_label, collected_messages)
            
            # Trigger search indexing
            index_messages_async.delay(snapshot_id, collected_messages[:100])  # Index first 100 for demo
            
            result = {
                'success': True,
                'tenant_name': tenant_data['name'],
                'snapshot_id': snapshot_id,
                'messages_collected': len(collected_messages),
                'messages_inserted': inserted_count,
                'backup_options': options
            }
            
            logger.info(f"Backup completed for tenant {tenant_data['name']}: {inserted_count} messages")
            return result
        else:
            return {
                'success': True,
                'tenant_name': tenant_data['name'],
                'snapshot_id': None,
                'messages_collected': 0,
                'messages_inserted': 0,
                'message': 'No messages found to backup'
            }
            
    except Exception as exc:
        logger.error(f"Backup failed for tenant {tenant_data['name']}: {str(exc)}")
        self.update_state(
            state='FAILURE',
            meta={'stage': 'failed', 'progress': 0, 'error': str(exc)}
        )
        raise exc


@celery_app.task(bind=True)
def backup_all_tenants_async(self, options: Dict[str, Any]) -> Dict[str, Any]:
    """
    Asynchronously backup all configured tenants.
    
    Args:
        options: Backup options (mails_per_user, download_attachments, label)
    
    Returns:
        Dict containing overall backup results
    """
    try:
        logger.info("Starting backup for all tenants")
        
        _db = db.DatabaseOperations()
        tenants = _db.get_tenants_for_backup()
        
        if not tenants:
            return {'success': False, 'error': 'No tenants configured'}
        
        self.update_state(
            state='PROGRESS',
            meta={'stage': 'starting', 'progress': 5, 'message': f'Found {len(tenants)} tenants to backup'}
        )
        
        all_collected = []
        tenant_results = []
        
        for i, tenant in enumerate(tenants):
            progress = 10 + (i * 70 // len(tenants))
            self.update_state(
                state='PROGRESS',
                meta={
                    'stage': 'backing_up', 
                    'progress': progress, 
                    'message': f'Backing up tenant: {tenant["name"]} ({i+1}/{len(tenants)})'
                }
            )
            
            try:
                collected = main.backup_tenant(tenant, options)
                if collected:
                    all_collected.extend(collected)
                    tenant_results.append({
                        'name': tenant['name'],
                        'messages': len(collected),
                        'success': True
                    })
                else:
                    tenant_results.append({
                        'name': tenant['name'],
                        'messages': 0,
                        'success': True,
                        'note': 'No messages found'
                    })
            except Exception as e:
                logger.error(f"Failed to backup tenant {tenant['name']}: {str(e)}")
                tenant_results.append({
                    'name': tenant['name'],
                    'messages': 0,
                    'success': False,
                    'error': str(e)
                })
        
        self.update_state(
            state='PROGRESS',
            meta={'stage': 'storing', 'progress': 85, 'message': 'Storing all messages in database...'}
        )
        
        if all_collected:
            snapshot_label = options.get('label', 'async-all-tenants')
            snapshot_id, inserted_count = _db.store_snapshot(snapshot_label, all_collected)
            
            # Trigger search indexing for a subset
            index_messages_async.delay(snapshot_id, all_collected[:200])
            
            return {
                'success': True,
                'snapshot_id': snapshot_id,
                'messages_collected': len(all_collected),
                'messages_inserted': inserted_count,
                'tenant_results': tenant_results,
                'backup_options': options
            }
        else:
            return {
                'success': False,
                'error': 'No messages collected from any tenant',
                'tenant_results': tenant_results
            }
            
    except Exception as exc:
        logger.error(f"Full backup failed: {str(exc)}")
        self.update_state(
            state='FAILURE', 
            meta={'stage': 'failed', 'progress': 0, 'error': str(exc)}
        )
        raise exc


@celery_app.task(bind=True)
def index_messages_async(self, snapshot_id: int, messages: List[Dict]) -> Dict[str, Any]:
    """
    Asynchronously index messages in Meilisearch for fast searching.
    
    Args:
        snapshot_id: ID of the snapshot these messages belong to
        messages: List of message dictionaries to index
        
    Returns:
        Dict containing indexing results
    """
    try:
        logger.info(f"Starting search indexing for snapshot {snapshot_id}, {len(messages)} messages")
        
        search_client = MeilisearchClient()
        
        self.update_state(
            state='PROGRESS',
            meta={'stage': 'indexing', 'progress': 20, 'message': 'Preparing messages for search index...'}
        )
        
        # Prepare messages for indexing (extract searchable content)
        searchable_messages = []
        for msg in messages:
            searchable_msg = {
                'id': f"snap_{snapshot_id}_msg_{hash(msg.get('message_id', ''))}",
                'snapshot_id': snapshot_id,
                'message_id': msg.get('message_id', ''),
                'subject': msg.get('subject', ''),
                'from_address': msg.get('from', ''),
                'to_addresses': ', '.join(msg.get('to', [])) if isinstance(msg.get('to'), list) else str(msg.get('to', '')),
                'received_datetime': msg.get('received_datetime', ''),
                'body_preview': msg.get('body_preview', '')[:500],  # Limit body preview
                'has_attachments': bool(msg.get('attachments')),
                'importance': msg.get('importance', 'normal'),
            }
            searchable_messages.append(searchable_msg)
        
        self.update_state(
            state='PROGRESS',
            meta={'stage': 'uploading', 'progress': 60, 'message': 'Uploading to search index...'}
        )
        
        # Index in Meilisearch
        result = search_client.index_messages(searchable_messages)
        
        logger.info(f"Search indexing completed for snapshot {snapshot_id}: {result}")
        
        return {
            'success': True,
            'snapshot_id': snapshot_id,
            'indexed_count': len(searchable_messages),
            'meilisearch_result': result
        }
        
    except Exception as exc:
        logger.error(f"Search indexing failed for snapshot {snapshot_id}: {str(exc)}")
        self.update_state(
            state='FAILURE',
            meta={'stage': 'failed', 'progress': 0, 'error': str(exc)}
        )
        raise exc


# Periodic tasks configuration
from celery.schedules import crontab

celery_app.conf.beat_schedule = {
    'daily-full-backup': {
        'task': 'm365_backup.tasks.backup_all_tenants_async',
        'schedule': crontab(hour=2, minute=0),  # Daily at 2 AM
        'args': [{'mails_per_user': 500, 'download_attachments': True, 'label': 'daily-auto'}]
    },
}
celery_app.conf.timezone = 'UTC'


if __name__ == '__main__':
    celery_app.start()