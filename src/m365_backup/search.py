"""
Meilisearch client for fast email search functionality.
"""
import os
from typing import List, Dict, Any, Optional
import meilisearch
from meilisearch.errors import MeilisearchApiError


class MeilisearchClient:
    """Client for interacting with Meilisearch for email indexing and searching."""
    
    def __init__(self):
        self.url = os.getenv('MEILISEARCH_URL', 'http://localhost:7700')
        self.key = os.getenv('MEILISEARCH_KEY', 'your-master-key-change-this')
        self.client = meilisearch.Client(self.url, self.key)
        self.index_name = 'm365_emails'
        self._setup_index()
    
    def _setup_index(self):
        """Setup the email search index with proper configuration."""
        try:
            # Create index if it doesn't exist
            try:
                self.index = self.client.get_index(self.index_name)
            except MeilisearchApiError:
                # Index doesn't exist, create it
                task = self.client.create_index(self.index_name, {'primaryKey': 'id'})
                self.index = self.client.get_index(self.index_name)
            
            # Configure searchable attributes
            self.index.update_searchable_attributes([
                'subject',
                'from_address', 
                'to_addresses',
                'body_preview'
            ])
            
            # Configure filterable attributes
            self.index.update_filterable_attributes([
                'snapshot_id',
                'received_datetime',
                'has_attachments',
                'importance',
                'from_address'
            ])
            
            # Configure sortable attributes
            self.index.update_sortable_attributes([
                'received_datetime',
                'subject'
            ])
            
            # Configure ranking rules for relevance
            self.index.update_ranking_rules([
                'words',
                'typo', 
                'proximity',
                'attribute',
                'sort',
                'exactness'
            ])
            
        except Exception as e:
            print(f"Error setting up Meilisearch index: {e}")
            raise
    
    def index_messages(self, messages: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Index a batch of messages in Meilisearch.
        
        Args:
            messages: List of message dictionaries to index
            
        Returns:
            Dictionary with indexing results
        """
        try:
            if not messages:
                return {'success': False, 'error': 'No messages to index'}
            
            # Add messages to index
            task = self.index.add_documents(messages)
            
            return {
                'success': True,
                'task_uid': task.task_uid,
                'indexed_count': len(messages),
                'status': 'enqueued'
            }
            
        except Exception as e:
            return {
                'success': False,
                'error': str(e),
                'indexed_count': 0
            }
    
    def search_messages(
        self, 
        query: str, 
        filters: Optional[Dict[str, Any]] = None,
        limit: int = 20,
        offset: int = 0,
        sort: Optional[List[str]] = None
    ) -> Dict[str, Any]:
        """
        Search for messages in the index.
        
        Args:
            query: Search query string
            filters: Optional filters (e.g., {'snapshot_id': 123})
            limit: Maximum number of results to return
            offset: Number of results to skip
            sort: Sort parameters
            
        Returns:
            Dictionary with search results
        """
        try:
            search_params = {
                'limit': limit,
                'offset': offset,
                'attributesToHighlight': ['subject', 'body_preview', 'from_address'],
                'highlightPreTag': '<mark>',
                'highlightPostTag': '</mark>',
            }
            
            if filters:
                # Convert filters to Meilisearch filter format
                filter_parts = []
                for key, value in filters.items():
                    if isinstance(value, str):
                        filter_parts.append(f'{key} = "{value}"')
                    else:
                        filter_parts.append(f'{key} = {value}')
                if filter_parts:
                    search_params['filter'] = ' AND '.join(filter_parts)
            
            if sort:
                search_params['sort'] = sort
            
            results = self.index.search(query, search_params)
            
            return {
                'success': True,
                'hits': results.get('hits', []),
                'total_hits': results.get('estimatedTotalHits', 0),
                'query': query,
                'processing_time_ms': results.get('processingTimeMs', 0),
                'limit': limit,
                'offset': offset
            }
            
        except Exception as e:
            return {
                'success': False,
                'error': str(e),
                'hits': [],
                'total_hits': 0
            }
    
    def get_facet_distribution(self, facets: List[str]) -> Dict[str, Any]:
        """
        Get distribution of values for specified facets.
        
        Args:
            facets: List of facet names to get distribution for
            
        Returns:
            Dictionary with facet distributions
        """
        try:
            results = self.index.search('', {
                'facets': facets,
                'limit': 0  # We only want facet data
            })
            
            return {
                'success': True,
                'facet_distribution': results.get('facetDistribution', {})
            }
            
        except Exception as e:
            return {
                'success': False,
                'error': str(e),
                'facet_distribution': {}
            }
    
    def delete_snapshot_messages(self, snapshot_id: int) -> Dict[str, Any]:
        """
        Delete all messages belonging to a specific snapshot.
        
        Args:
            snapshot_id: ID of the snapshot to delete messages for
            
        Returns:
            Dictionary with deletion results
        """
        try:
            # Delete by filter
            task = self.index.delete_documents_by_filter(f'snapshot_id = {snapshot_id}')
            
            return {
                'success': True,
                'task_uid': task.task_uid,
                'status': 'enqueued'
            }
            
        except Exception as e:
            return {
                'success': False,
                'error': str(e)
            }
    
    def get_index_stats(self) -> Dict[str, Any]:
        """
        Get statistics about the search index.
        
        Returns:
            Dictionary with index statistics
        """
        try:
            stats = self.index.get_stats()
            
            return {
                'success': True,
                'number_of_documents': stats.get('numberOfDocuments', 0),
                'is_indexing': stats.get('isIndexing', False),
                'field_distribution': stats.get('fieldDistribution', {})
            }
            
        except Exception as e:
            return {
                'success': False,
                'error': str(e),
                'number_of_documents': 0
            }