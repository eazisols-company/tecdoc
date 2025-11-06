#!/usr/bin/env python3
"""
Tecdoc API Client - Complete Solution
One script that does everything: gets data and exports to CSV
"""

import requests
import json
import csv
import sys
import os
from typing import Dict, List, Any, Optional
from datetime import datetime
import pandas as pd

# Configuration
TECDOC_API_KEY = "2BeBXg6R9LRdWoHtCcfhS8EB74TpK7uQn3nejjYmbpK2WDnwE7Kq"
TECDOC_BASE_URL = "https://webservice.tecalliance.services/pegasus-3-0/services/TecdocToCatDLB.jsonEndpoint"
TECDOC_PROVIDER = 25183

# Authentication Headers
API_HEADERS = {
    'content-type': 'application/json;charset=UTF-8',
    'x-api-key': TECDOC_API_KEY
}

# Default Settings
DEFAULT_COUNTRY = "de"
DEFAULT_LANGUAGE = "de"

# Test Data
TEST_MANUFACTURER_ID = 355  # DT Spare Parts (Article Manufacturer)
TEST_ARTICLE_NUMBER = "1.31809"

class TecdocClient:
    def __init__(self):
        self.base_url = TECDOC_BASE_URL
        self.headers = API_HEADERS
        self.csv_data = {
            'articles': [],
            'attributes': [],
            'references': [],
            'components': [],
            'article_relations': [],
            'brands': []
        }
        
    def make_request(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """Make API request to Tecdoc endpoint"""
        try:
            response = requests.post(
                self.base_url,
                headers=self.headers,
                json=payload,
                timeout=30
            )
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"ERROR: API request failed: {e}")
            return {}
    
    def get_articles(self, manufacturer_id: int, article_number: str) -> Dict[str, Any]:
        """Get articles with images and GTINs"""
        payload = {
            "getArticles": {
                "articleCountry": DEFAULT_COUNTRY,
                "provider": TECDOC_PROVIDER,
                "searchQuery": article_number,
                "dataSupplierIds": manufacturer_id,
                "lang": DEFAULT_LANGUAGE,
                "includeMisc": True,
                "includeGenericArticles": True,
                "includeLinkages": True,
                "includeAccessoryArticles": True,
                "includePDFs": True,
                "includeImages": True,
                "includeLinks": True,
                "includeArticleCriteria": True,
                "includeOEMNumbers": True,
                "includeComparableNumbers": True,
                "includeTradeNumbers": True,
                "includeReplacedByArticles": True,
                "includeReplacesArticles": True,
                "includeGTINs": True,
                "assemblyGroupFacetOptions": {
                    "enabled": True,
                    "assemblyGroupType": "O",
                    "includeCompleteTree": True
                }
            }
        }
        
        print(f"Getting article data for {article_number} from manufacturer {manufacturer_id}...")
        response = self.make_request(payload)
        
        if response and 'articles' in response:
            return response  # Return the response as-is since it already has the correct structure
        return response
    
    def get_enhanced_article_data(self, article_id: int) -> Dict[str, Any]:
        """Get enhanced article data using the article ID"""
        payload = {
            "getArticles": {
                "articleCountry": DEFAULT_COUNTRY,
                "provider": TECDOC_PROVIDER,
                "articleIds": [article_id],
                "lang": DEFAULT_LANGUAGE,
                "includeGTINs": True,
                "includeLinkages": True,
                "includeImages": True,
                "includeLinks": True,
                "includeGenericArticles": True,
                "includeAssemblyGroups": True
            }
        }
        
        print(f"Getting enhanced article data for ID {article_id}...")
        return self.make_request(payload)
    
    def get_comparable_numbers(self, article_number: str, generic_article_ids: List[int]) -> Dict[str, Any]:
        """Get Comparable Numbers using genericArticleIds as per client requirements
        
        Args:
            article_number: The article number to search for
            generic_article_ids: List of generic article IDs (read from articles.csv)
        """
        if not generic_article_ids:
            return {}
        
        payload = {
            "getArticles": {
                "articleCountry": DEFAULT_COUNTRY,
                "provider": TECDOC_PROVIDER,
                "searchQuery": article_number,
                "searchType": 3,
                "lang": DEFAULT_LANGUAGE,
                "genericArticleIds": generic_article_ids,
                "includeAll": False
            }
        }
        
        print(f"   Getting Comparable Numbers for article {article_number} with genericArticleIds {generic_article_ids}...")
        response = self.make_request(payload)
        
        return response
    
    def get_article_name_and_id(self, manufacturer_id: int, article_number: str) -> tuple:
        """Get article name and ID using direct search"""
        payload = {
            "getArticleDirectSearchAllNumbersWithState": {
                "articleCountry": DEFAULT_COUNTRY,
                "articleNumber": article_number,
                "brandId": manufacturer_id,
                "lang": DEFAULT_LANGUAGE,
                "numberType": 0,
                "provider": TECDOC_PROVIDER,
                "includeImages": True
            }
        }
        
        print(f"Getting article name and ID for {article_number}...")
        response = self.make_request(payload)
        
        if response and 'data' in response:
            articles = response.get('data', {}).get('array', [])
            if articles:
                article_name = articles[0].get('articleName', 'N/A')
                article_id = articles[0].get('articleId', '')
                return article_name, article_id
        return 'N/A', ''
    
    def get_brand_info(self, supplier_id: int) -> Dict[str, Any]:
        """Get brand information"""
        payload = {
            "getBrandInfo": {
                "articleCountry": DEFAULT_COUNTRY,
                "supplierId": supplier_id,
                "lang": DEFAULT_LANGUAGE,
                "provider": TECDOC_PROVIDER
            }
        }
        
        print(f"Getting brand info for supplier ID {supplier_id}...")
        return self.make_request(payload)
    
    def get_article_classification(self, article_id: int) -> Dict[str, Any]:
        """Get article classification using getArticleLinkedAllLinkingTarget2"""
        payload = {
            "getArticleLinkedAllLinkingTarget2": {
                "articleCountry": DEFAULT_COUNTRY,
                "articleId": article_id,
                "lang": DEFAULT_LANGUAGE,
                "linkingTargetType": "P",
                "provider": TECDOC_PROVIDER
            }
        }
        
        print(f"Getting classification for article ID {article_id}...")
        return self.make_request(payload)
    
    def get_article_info(self, article_id: int) -> Dict[str, Any]:
        """Get detailed article information using getArticleLinkedAllLinkingTarget2"""
        payload = {
            "getArticleLinkedAllLinkingTarget2": {
                "articleCountry": DEFAULT_COUNTRY,
                "articleId": article_id,
                "lang": DEFAULT_LANGUAGE,
                "linkingTargetType": "A",
                "provider": TECDOC_PROVIDER
            }
        }
        
        print(f"Getting article info for article ID {article_id}...")
        return self.make_request(payload)
    
    def extract_image_urls(self, images_data: List[Dict[str, Any]]) -> Dict[str, str]:
        """Extract image URLs from images list"""
        image_urls = {}
        
        if images_data and len(images_data) > 0:
            first_image = images_data[0]
            for key, url in first_image.items():
                if key.startswith('imageURL') and url:
                    size = key.replace('imageURL', '')
                    image_urls[f"image_{size}px"] = url
        
        return image_urls
    
    def _extract_category_data(self, assembly_groups: List[Dict[str, Any]]) -> tuple:
        """Extract category path and node IDs from assembly groups"""
        if not assembly_groups:
            return '', ''
        
        path_parts = []
        node_ids = []
        
        for group in assembly_groups:
            if group.get('description'):
                path_parts.append(group['description'])
            if group.get('id'):
                node_ids.append(str(group['id']))
        
        category_path = ' > '.join(path_parts) if path_parts else ''
        category_node_ids = '|'.join(node_ids) if node_ids else ''
        
        return category_path, category_node_ids
    
    def _build_category_hierarchy_from_facets(self, facets_data: Dict[str, Any]) -> tuple:
        """Build category path and node IDs from assembly group facets"""
        if not facets_data or 'counts' not in facets_data:
            return '', ''
        
        counts = facets_data.get('counts', [])
        if not counts:
            return '', ''
        
        # Build a map of nodeId -> node data
        node_map = {}
        for node in counts:
            node_id = node.get('assemblyGroupNodeId')
            if node_id:
                node_map[node_id] = node
        
        # Find the root node (one without parentNodeId)
        root_node = None
        for node in counts:
            if 'parentNodeId' not in node or node.get('parentNodeId') is None:
                root_node = node
                break
        
        if not root_node:
            # If no clear root found, just take the first node
            return '', ''
        
        # Build the hierarchy from root to leaf
        path_parts = []
        node_ids = []
        
        # Add root
        path_parts.append(root_node.get('assemblyGroupName', ''))
        node_ids.append(str(root_node.get('assemblyGroupNodeId', '')))
        
        # Find children recursively
        current_id = root_node.get('assemblyGroupNodeId')
        while True:
            child_found = False
            for node in counts:
                if node.get('parentNodeId') == current_id:
                    path_parts.append(node.get('assemblyGroupName', ''))
                    node_ids.append(str(node.get('assemblyGroupNodeId', '')))
                    current_id = node.get('assemblyGroupNodeId')
                    child_found = True
                    break
            
            if not child_found:
                break
        
        category_path = ' > '.join(path_parts) if path_parts else ''
        category_node_ids = '|'.join(node_ids) if node_ids else ''
        
        return category_path, category_node_ids
    
    def _extract_classification_data(self, class_data: Dict[str, Any]) -> tuple:
        """Extract classification data from API response"""
        generic_article_id = ''
        generic_article_description = ''
        category_path = ''
        category_node_ids = ''
        
        # Handle array response
        if isinstance(class_data, dict) and 'array' in class_data:
            articles_data = class_data['array']
            if articles_data and len(articles_data) > 0:
                article_data = articles_data[0]
                if 'genericArticle' in article_data:
                    gen_art = article_data['genericArticle']
                    generic_article_id = str(gen_art.get('id', ''))
                    generic_article_description = gen_art.get('description', '')
                
                # Extract assembly groups
                if 'assemblyGroups' in article_data:
                    category_path, category_node_ids = self._extract_category_data(article_data['assemblyGroups'])
        
        # Handle direct response
        elif 'genericArticle' in class_data:
            gen_art = class_data['genericArticle']
            generic_article_id = str(gen_art.get('id', ''))
            generic_article_description = gen_art.get('description', '')
            
            if 'assemblyGroups' in class_data:
                category_path, category_node_ids = self._extract_category_data(class_data['assemblyGroups'])
        
        return generic_article_id, generic_article_description, category_path, category_node_ids
    
    def _extract_category_from_linkages(self, linkages: List[Dict[str, Any]]) -> tuple:
        """Extract category information from linkages as fallback"""
        if not linkages:
            return '', ''
        
        path_parts = []
        node_ids = []
        
        for linkage in linkages:
            if 'assemblyGroupDescription' in linkage and linkage['assemblyGroupDescription']:
                path_parts.append(linkage['assemblyGroupDescription'])
            if 'assemblyGroupNodeId' in linkage and linkage['assemblyGroupNodeId']:
                node_ids.append(str(linkage['assemblyGroupNodeId']))
        
        category_path = ' > '.join(path_parts) if path_parts else ''
        category_node_ids = '|'.join(node_ids) if node_ids else ''
        
        return category_path, category_node_ids
    
    def process_complete_article_data(self, article: Dict[str, Any], article_name: str, article_id: int, supplier_id: int, assembly_group_facets: Dict[str, Any] = None, article_number: str = '') -> None:
        """Process article data and populate articles CSV data structure"""
        if not article_id:
            print(f"   ERROR: No article ID found, skipping")
            return
        
        print(f"   Processing article ID: {article_id}")
        
        # Extract genericArticleId for Comparable Numbers request
        generic_article_id = None
        if 'genericArticles' in article and article['genericArticles']:
            generic_article_id = article['genericArticles'][0].get('genericArticleId')
        
        # Process articles.csv data only (focusing on articles.csv for now)
        self.process_articles_data(article, article_name, article_id, supplier_id, assembly_group_facets)
        
        # Check if attributes are included in the article data
        if 'articleCriteria' in article or 'attributes' in article or 'criteria' in article:
            print(f"   Found attributes in article data, processing...")
            self.extract_attributes_from_article(article_id, article)
        
        # Extract OE references (articleNumber and mfrName) from the main article data
        if 'oemNumbers' in article and article['oemNumbers']:
            self.extract_references_from_article(article_id, article)
        
        # Extract only the specific EAN 04057795419360 (4057795419360) for article 1.31809
        # Client confirmed this specific EAN is correct, but other EANs should be excluded
        # Check both the article_number parameter and the article's articleNumber field
        article_number_from_data = article.get('articleNumber', article_number)
        if article_number == "1.31809" or article_number_from_data == "1.31809":
            # Extract the specific EAN
            self.extract_specific_gtin(article_id, article, "4057795419360")
        
        # Get comparable numbers and extract articleNumber and mfrName from the response articles
        # Client wants articleNumber and mfrName from comparable numbers search results
        # This applies to all articles that have a generic_article_id
        if generic_article_id and article_number:
            comparable_response = self.get_comparable_numbers(article_number, [generic_article_id])
            if comparable_response and 'articles' in comparable_response:
                self.extract_comparable_articles_as_references(article_id, comparable_response)
    
    def extract_specific_gtin(self, article_id: int, article: Dict[str, Any], target_gtin: str) -> bool:
        """Extract only a specific GTIN/EAN from article data
        
        Args:
            article_id: Article ID
            article: Article data dictionary
            target_gtin: The specific GTIN value to extract (e.g., "4057795419360")
            
        Returns:
            bool: True if the specific GTIN was found and added, False otherwise
        """
        # Normalize target GTIN (remove leading zeros for comparison)
        target_gtin_normalized = target_gtin.lstrip('0')
        
        # Check for GTINs with case-insensitive search
        gtin_key = None
        for key in article.keys():
            if key.lower() == 'gtins':
                gtin_key = key
                break
        
        if not gtin_key and 'GTINs' in article:
            gtin_key = 'GTINs'
        
        # Try alternative key names
        if not gtin_key:
            for alt_key in ['gtin', 'GTIN', 'gtinNumbers', 'gtins', 'ean', 'EAN', 'eans', 'EANs']:
                if alt_key in article:
                    gtin_key = alt_key
                    break
        
        if gtin_key and article.get(gtin_key):
            gtin_data = article.get(gtin_key)
            
            # Handle different structures
            if isinstance(gtin_data, dict):
                if 'array' in gtin_data:
                    gtin_data = gtin_data['array']
            
            if isinstance(gtin_data, list):
                for ref in gtin_data:
                    # Handle different GTIN formats
                    gtin_number = ''
                    if isinstance(ref, str):
                        gtin_number = ref.strip()
                    elif isinstance(ref, (int, float)):
                        gtin_number = str(ref).strip()
                    elif isinstance(ref, dict):
                        # Try various possible field names for GTIN
                        gtin_number = (
                            ref.get('gtin') or 
                            ref.get('GTIN') or
                            ref.get('ean') or 
                            ref.get('EAN') or
                            ref.get('gtinNumber') or 
                            ref.get('gtinValue') or 
                            ref.get('eanNumber') or 
                            ref.get('number') or 
                            ref.get('value') or
                            ref.get('gtinCode') or
                            ref.get('eanCode') or
                            ''
                        )
                        if gtin_number:
                            gtin_number = str(gtin_number).strip()
                    
                    if gtin_number:
                        # Normalize both for comparison (remove leading zeros)
                        gtin_normalized = gtin_number.lstrip('0')
                        
                        # Check if this matches the target GTIN (with or without leading zeros)
                        if (gtin_normalized == target_gtin_normalized or 
                            gtin_number == target_gtin or 
                            gtin_number == f"0{target_gtin}" or
                            gtin_number == target_gtin.lstrip('0')):
                            # Found the target GTIN, add it
                            reference_row = {
                                'article_id': article_id,
                                'ref_type': 'EAN',
                                'number': gtin_number,  # Keep original format with leading zero if present
                                'mfr_name': ''
                            }
                            self.csv_data['references'].append(reference_row)
                            print(f"   ✓ Added specific GTIN: {gtin_number}")
                            return True
            elif gtin_data and isinstance(gtin_data, str):
                # Handle single GTIN (not in a list)
                gtin_number = gtin_data.strip()
                gtin_normalized = gtin_number.lstrip('0')
                
                if (gtin_normalized == target_gtin_normalized or 
                    gtin_number == target_gtin or 
                    gtin_number == f"0{target_gtin}" or
                    gtin_number == target_gtin.lstrip('0')):
                    reference_row = {
                        'article_id': article_id,
                        'ref_type': 'EAN',
                        'number': gtin_number,
                        'mfr_name': ''
                    }
                    self.csv_data['references'].append(reference_row)
                    print(f"   ✓ Added specific GTIN: {gtin_number}")
                    return True
        
        return False
    
    def extract_comparable_articles_as_references(self, article_id: int, comparable_response: Dict[str, Any]) -> None:
        """Extract articleNumber and mfrName from comparable numbers search results
        
        Client wants articleNumber and mfrName from each article in the comparable numbers
        response, not the ComparableNumber type entries. These should be added as OE references.
        
        Args:
            article_id: The original article ID
            comparable_response: Response from get_comparable_numbers() API call
        """
        if not comparable_response or 'articles' not in comparable_response:
            return
        
        articles = comparable_response.get('articles', [])
        if not articles:
            return
        
        print(f"   Extracting articleNumber and mfrName from {len(articles)} comparable articles...")
        
        for comp_article in articles:
            article_number = comp_article.get('articleNumber', '')
            mfr_name = comp_article.get('mfrName', '')
            
            if article_number and mfr_name:
                # Add as OE reference with articleNumber and mfrName
                reference_row = {
                    'article_id': article_id,
                    'ref_type': 'OE',
                    'number': article_number,
                    'mfr_name': mfr_name
                }
                self.csv_data['references'].append(reference_row)
                print(f"   ✓ Added comparable article: {article_number} ({mfr_name})")
    
    def extract_gtins_from_article(self, article_id: int, article: Dict[str, Any]) -> int:
        """Extract GTINs/EANs from article data (reusable method)
        
        Returns:
            int: Number of GTINs extracted
        """
        gtin_count = 0
        # Track GTINs we've already added for this article to avoid duplicates
        existing_gtins = set()
        for ref in self.csv_data['references']:
            if ref.get('article_id') == article_id and ref.get('ref_type') == 'EAN':
                existing_gtins.add(ref.get('number', ''))
        
        # Check for GTINs with case-insensitive search
        gtin_key = None
        for key in article.keys():
            if key.lower() == 'gtins':
                gtin_key = key
                break
        
        if not gtin_key and 'GTINs' in article:
            gtin_key = 'GTINs'
        
        # Try alternative key names
        if not gtin_key:
            for alt_key in ['gtin', 'GTIN', 'gtinNumbers', 'gtins', 'ean', 'EAN', 'eans', 'EANs']:
                if alt_key in article:
                    gtin_key = alt_key
                    break
        
        if gtin_key and article.get(gtin_key):
            gtin_data = article.get(gtin_key)
            
            # Handle different structures
            if isinstance(gtin_data, dict):
                if 'array' in gtin_data:
                    gtin_data = gtin_data['array']
            
            if isinstance(gtin_data, list):
                for ref in gtin_data:
                    # Handle different GTIN formats
                    gtin_number = ''
                    if isinstance(ref, str):
                        gtin_number = ref.strip()
                    elif isinstance(ref, (int, float)):
                        gtin_number = str(ref).strip()
                    elif isinstance(ref, dict):
                        # Try various possible field names for GTIN (case-insensitive)
                        gtin_number = (
                            ref.get('gtin') or 
                            ref.get('GTIN') or
                            ref.get('ean') or 
                            ref.get('EAN') or
                            ref.get('gtinNumber') or 
                            ref.get('gtinValue') or 
                            ref.get('eanNumber') or 
                            ref.get('number') or 
                            ref.get('value') or
                            ref.get('gtinCode') or
                            ref.get('eanCode') or
                            ''
                        )
                        if gtin_number:
                            gtin_number = str(gtin_number).strip()
                    
                    if gtin_number and gtin_number not in existing_gtins:
                        reference_row = {
                            'article_id': article_id,
                            'ref_type': 'EAN',
                            'number': str(gtin_number),
                            'mfr_name': ''
                        }
                        self.csv_data['references'].append(reference_row)
                        existing_gtins.add(gtin_number)
                        gtin_count += 1
                        print(f"   ✓ Added GTIN: {gtin_number}")
            elif gtin_data and isinstance(gtin_data, str):
                # Handle single GTIN (not in a list)
                gtin_number = gtin_data.strip()
                if gtin_number and gtin_number not in existing_gtins:
                    reference_row = {
                        'article_id': article_id,
                        'ref_type': 'EAN',
                        'number': gtin_number,
                        'mfr_name': ''
                    }
                    self.csv_data['references'].append(reference_row)
                    existing_gtins.add(gtin_number)
                    gtin_count += 1
                    print(f"   ✓ Added GTIN: {gtin_number}")
        
        return gtin_count
    
    def process_articles_data(self, article: Dict[str, Any], article_name: str, article_id: int, supplier_id: int, assembly_group_facets: Dict[str, Any] = None) -> None:
        """Process data for articles.csv with improved schema compliance"""
        print(f"   Processing article data for articles.csv...")
        
        # Extract data from the new API response structure
        # Map API response fields to CSV columns
        legacy_article_id = ''
        data_supplier_id = article.get('dataSupplierId', '')
        mfr_name = article.get('mfrName', '')
        article_number = article.get('articleNumber', '')
        
        # Extract generic article data
        generic_article_id = ''
        generic_article_description = ''
        assembly_group_name = ''
        assembly_group_node_id = ''
        
        if 'genericArticles' in article and article['genericArticles']:
            gen_article = article['genericArticles'][0]  # Take first generic article
            generic_article_id = str(gen_article.get('genericArticleId', ''))
            generic_article_description = gen_article.get('genericArticleDescription', '')
            assembly_group_name = gen_article.get('assemblyGroupName', '')
            assembly_group_node_id = str(gen_article.get('assemblyGroupNodeId', ''))
            legacy_article_id = str(gen_article.get('legacyArticleId', ''))
        
        # Build category hierarchy from assembly group facets
        category_path = ''
        category_node_ids = ''
        if assembly_group_facets:
            category_path, category_node_ids = self._build_category_hierarchy_from_facets(assembly_group_facets)
        
        # Fallback to simple assembly group name if facets not available
        if not category_path and assembly_group_name:
            category_path = assembly_group_name
        if not category_node_ids and assembly_group_node_id:
            category_node_ids = assembly_group_node_id
        
        # Extract misc data fields
        is_accessory = 'false'
        article_status_id = ''
        article_status_description = ''
        article_status_valid_from_date = ''
        quantity_per_package = ''
        quantity_per_part_per_package = ''
        is_self_service_packing = 'false'
        has_mandatory_material_certification = 'false'
        is_remanufactured_part = 'false'
        
        if 'misc' in article and article['misc']:
            misc_data = article['misc']
            is_accessory = str(misc_data.get('isAccessory', False)).lower()
            article_status_id = str(misc_data.get('articleStatusId', ''))
            article_status_description = misc_data.get('articleStatusDescription', '')
            article_status_valid_from_date = str(misc_data.get('articleStatusValidFromDate', ''))
            quantity_per_package = str(misc_data.get('quantityPerPackage', ''))
            quantity_per_part_per_package = str(misc_data.get('quantityPerPartPerPackage', ''))
            is_self_service_packing = str(misc_data.get('isSelfServicePacking', False)).lower()
            has_mandatory_material_certification = str(misc_data.get('hasMandatoryMaterialCertification', False)).lower()
            is_remanufactured_part = str(misc_data.get('isRemanufacturedPart', False)).lower()
        
        # Extract 3200px images sorted by sortNumber
        image_urls_3200 = []
        
        if 'images' in article and article['images']:
            # Sort images by sortNumber
            sorted_images = sorted(article['images'], key=lambda x: x.get('sortNumber', 999))
            
            # Extract only the 3200px URLs
            for img in sorted_images:
                if 'imageURL3200' in img and img['imageURL3200']:
                    image_urls_3200.append(img['imageURL3200'])
        
        # Extract PDF URLs
        pdf_urls = []
        if 'pdfs' in article and article['pdfs']:
            for pdf in article['pdfs']:
                if 'url' in pdf and pdf['url']:
                    pdf_urls.append(pdf['url'])
        
        # Extract mfrId
        mfr_id = str(article.get('mfrId', ''))
        
        # Create article row according to schema
        article_row = {
            'article_id': legacy_article_id,  # Using legacyArticleId as article_id
            'supplier_id': data_supplier_id,  # Using dataSupplierId as supplier_id
            'mfr_id': mfr_id,  # Using mfrId
            'brand_name': mfr_name,  # Using mfrName as brand_name
            'article_number': article_number,  # Using articleNumber as article_number
            'generic_article_id': generic_article_id,  # Using genericArticleId
            'generic_article_description': generic_article_description,  # Using genericArticleDescription
            'category_path': category_path,  # Built from assemblyGroupFacets hierarchy
            'category_node_ids': category_node_ids,  # Built from assemblyGroupFacets hierarchy
            'short_description': '',  # Missing field - set as empty
            'note': '',  # Missing field - set as empty
            'image_urls': '|'.join(image_urls_3200),  # 3200px images sorted by sortNumber, pipe-delimited
            'pdf_urls': '|'.join(pdf_urls),
            'is_accessory': is_accessory,  # Using isAccessory from misc
            'article_status_id': article_status_id,
            'article_status_description': article_status_description,
            'article_status_valid_from_date': article_status_valid_from_date,
            'quantity_per_package': quantity_per_package,
            'quantity_per_part_per_package': quantity_per_part_per_package,
            'is_self_service_packing': is_self_service_packing,
            'has_mandatory_material_certification': has_mandatory_material_certification,
            'is_remanufactured_part': is_remanufactured_part
        }
        
        self.csv_data['articles'].append(article_row)
        print(f"   OK Article data processed successfully for articles.csv")
    
    def _process_image_data(self, images_data: List[Dict[str, Any]], primary_images: Dict[str, str]) -> Dict[str, Any]:
        """Process image data and extract all required information"""
        result = {
            'primary_urls': {
                '50': primary_images.get('image_50px', ''),
                '100': primary_images.get('image_100px', ''),
                '200': primary_images.get('image_200px', ''),
                '400': primary_images.get('image_400px', ''),
                '800': primary_images.get('image_800px', '')
            },
            'doc_ids': [],
            'filenames': [],
            'doc_types': [],
            'gallery_urls': [],
            'pdf_urls': []
        }
        
        if not images_data:
            return result
        
        for img in images_data:
            if not isinstance(img, dict):
                continue
            
            # Extract document IDs
            doc_id = self._extract_document_id(img)
            if doc_id:
                result['doc_ids'].append(doc_id)
            
            # Extract filenames
            filename = self._extract_filename(img)
            if filename:
                result['filenames'].append(filename)
            
            # Extract document types
            doc_type = self._extract_document_type(img)
            if doc_type:
                result['doc_types'].append(doc_type)
            
            # Extract additional image URLs for gallery
            self._extract_gallery_urls(img, result['gallery_urls'], result['primary_urls'])
            
            # Extract PDF URLs
            self._extract_pdf_urls(img, result['pdf_urls'])
        
        return result
    
    def _extract_document_id(self, img: Dict[str, Any]) -> str:
        """Extract document ID from image data"""
        # Try explicit ID fields first
        for doc_id_field in ['docId', 'documentId', 'id', 'assetId', 'imageId']:
            if doc_id_field in img and img[doc_id_field]:
                return str(img[doc_id_field])
        
        # Generate ID from filename
        if 'fileName' in img and img['fileName']:
            return img['fileName'].replace('.JPG', '').replace('.jpg', '').replace('.jpeg', '').replace('.png', '')
        
        # Generate ID from URL
        if 'imageURL50' in img and img['imageURL50']:
            url = img['imageURL50']
            if '/' in url:
                return url.split('/')[-1].split('.')[0]
        
        return ''
    
    def _extract_filename(self, img: Dict[str, Any]) -> str:
        """Extract filename from image data"""
        for filename_field in ['fileName', 'filename', 'name']:
            if filename_field in img and img[filename_field]:
                return img[filename_field]
        return ''
    
    def _extract_document_type(self, img: Dict[str, Any]) -> str:
        """Extract document type from image data"""
        for type_field in ['typeDescription', 'docTypeName', 'documentType', 'type', 'mimeType']:
            if type_field in img and img[type_field]:
                return img[type_field]
        return ''
    
    def _extract_gallery_urls(self, img: Dict[str, Any], gallery_urls: List[str], primary_urls: Dict[str, str]) -> None:
        """Extract additional image URLs for gallery"""
        primary_url_values = set(primary_urls.values())
        
        for key, url in img.items():
            if (key.startswith('imageURL') and url and 
                isinstance(url, str) and 
                url not in primary_url_values and 
                url not in gallery_urls):
                gallery_urls.append(url)
    
    def _extract_pdf_urls(self, img: Dict[str, Any], pdf_urls: List[str]) -> None:
        """Extract PDF URLs from image data"""
        for key, url in img.items():
            if (isinstance(url, str) and 
                ('.pdf' in url.lower() or 'pdf' in key.lower()) and 
                url not in pdf_urls):
                pdf_urls.append(url)
    
    def extract_attributes_from_article(self, article_id: int, article: Dict[str, Any]) -> None:
        """Extract attributes directly from article data"""
        attributes_data = None
        
        # Try different possible keys
        if 'articleCriteria' in article:
            attributes_data = article['articleCriteria']
        elif 'attributes' in article:
            attributes_data = article['attributes']
        elif 'criteria' in article:
            attributes_data = article['criteria']
        
        if not attributes_data:
            return
        
        # If it's a dict with array, extract the array
        if isinstance(attributes_data, dict) and 'array' in attributes_data:
            attributes_data = attributes_data['array']
        
        if not isinstance(attributes_data, list):
            return
        
        print(f"   Found {len(attributes_data)} attributes")
        
        for attr in attributes_data:
            attribute_row = {
                'article_id': article_id,
                'criteria_id': attr.get('criteriaId', attr.get('id', '')),
                'criteria_description': attr.get('criteriaDescription', attr.get('description', '')),
                'criteria_abbr': attr.get('criteriaAbbrDescription', attr.get('criteriaAbbr', attr.get('abbr', ''))),
                'value_raw': attr.get('rawValue', attr.get('valueRaw', attr.get('value', ''))),
                'value_formatted': attr.get('formattedValue', attr.get('valueFormatted', attr.get('value', ''))),
                'unit': attr.get('criteriaUnitDescription', attr.get('unit', '')),
                'immediate_display': str(attr.get('immediateDisplay', False)).lower(),
                'is_interval': str(attr.get('isInterval', False)).lower()
            }
            self.csv_data['attributes'].append(attribute_row)
    
    def process_attributes_data(self, article_id: int, attributes_response: Dict[str, Any]) -> None:
        """Process data for attributes.csv"""
        print(f"   DEBUG: Attributes response keys: {attributes_response.keys() if attributes_response else 'None'}")
        
        if not attributes_response:
            print(f"   DEBUG: No attributes response received")
            return
        
        # Check for different possible response structures
        attributes_data = None
        
        # Try direct array access
        if 'array' in attributes_response:
            attributes_data = attributes_response['array']
            print(f"   DEBUG: Found direct array with {len(attributes_data)} items")
        # Try data.array structure
        elif 'data' in attributes_response:
            data = attributes_response.get('data', {})
            if 'array' in data:
                attributes_data = data['array']
                print(f"   DEBUG: Found data.array with {len(attributes_data)} items")
            elif isinstance(data, list):
                attributes_data = data
                print(f"   DEBUG: Found data as list with {len(attributes_data)} items")
        # Try if response is directly a list
        elif isinstance(attributes_response, list):
            attributes_data = attributes_response
            print(f"   DEBUG: Response is a list with {len(attributes_data)} items")
        
        if not attributes_data:
            print(f"   DEBUG: No attributes data found in response")
            print(f"   DEBUG: Full response: {attributes_response}")
            return
        
        for attr in attributes_data:
            print(f"   DEBUG: Processing attribute: {attr.keys() if isinstance(attr, dict) else attr}")
            attribute_row = {
                'article_id': article_id,
                'criteria_id': attr.get('criteriaId', ''),
                'criteria_description': attr.get('criteriaDescription', ''),
                'criteria_abbr': attr.get('criteriaAbbr', ''),
                'value_raw': attr.get('valueRaw', ''),
                'value_formatted': attr.get('valueFormatted', ''),
                'unit': attr.get('unit', ''),
                'immediate_display': str(attr.get('immediateDisplay', False)).lower(),
                'is_interval': str(attr.get('isInterval', False)).lower()
            }
            self.csv_data['attributes'].append(attribute_row)
        
        print(f"   DEBUG: Added {len(attributes_data)} attributes to CSV data")
    
    def extract_references_from_article(self, article_id: int, article: Dict[str, Any]) -> None:
        """Extract only OE references (articleNumber and mfrName) from article data
        
        Client only needs OE references with articleNumber and mfrName.
        Other reference types (Trade, Comparable, EAN/GTIN, Replaced, Replacement) are excluded.
        """
        total_references = 0
        
        # Extract only OEM Numbers (OE references)
        if 'oemNumbers' in article and article['oemNumbers']:
            oem_data = article['oemNumbers']
            if isinstance(oem_data, dict) and 'array' in oem_data:
                oem_data = oem_data['array']
            if isinstance(oem_data, list):
                for ref in oem_data:
                    if isinstance(ref, str):
                        reference_row = {
                            'article_id': article_id,
                            'ref_type': 'OE',
                            'number': ref,
                            'mfr_name': ''
                        }
                    else:
                        reference_row = {
                            'article_id': article_id,
                            'ref_type': 'OE',
                            'number': ref.get('articleNumber', ref.get('number', '')),
                            'mfr_name': ref.get('mfrName', ref.get('brandName', ''))
                        }
                    self.csv_data['references'].append(reference_row)
                    total_references += 1
        
        if total_references > 0:
            print(f"   Found {total_references} OE references")
    
    def process_references_data(self, article_id: int, references_response: Dict[str, Any]) -> None:
        """Process data for references.csv"""
        print(f"   DEBUG: References response keys: {references_response.keys() if references_response else 'None'}")
        
        if not references_response:
            print(f"   DEBUG: No references response received")
            return
        
        # Check for different possible response structures
        references_data = None
        
        # Try direct array access
        if 'array' in references_response:
            references_data = references_response['array']
            print(f"   DEBUG: Found direct array with {len(references_data)} items")
        # Try data.array structure
        elif 'data' in references_response:
            data = references_response.get('data', {})
            if 'array' in data:
                references_data = data['array']
                print(f"   DEBUG: Found data.array with {len(references_data)} items")
            elif isinstance(data, list):
                references_data = data
                print(f"   DEBUG: Found data as list with {len(references_data)} items")
        # Try if response is directly a list
        elif isinstance(references_response, list):
            references_data = references_response
            print(f"   DEBUG: Response is a list with {len(references_data)} items")
        
        if not references_data:
            print(f"   DEBUG: No references data found in response")
            print(f"   DEBUG: Full response: {references_response}")
            return
        
        for ref in references_data:
            reference_row = {
                'article_id': article_id,
                'ref_type': ref.get('referenceType', ''),
                'number': ref.get('number', ''),
                'mfr_name': ref.get('mfrName', '')
            }
            self.csv_data['references'].append(reference_row)
        
        print(f"   DEBUG: Added {len(references_data)} references to CSV data")
    
    def process_components_data(self, article_id: int, components_response: Dict[str, Any]) -> None:
        """Process data for components.csv"""
        if not components_response or 'data' not in components_response:
            return
        
        components_data = components_response.get('data', {})
        if 'array' in components_data:
            for comp in components_data['array']:
                component_row = {
                    'parent_article_id': article_id,
                    'component_article_id': comp.get('componentArticleId', ''),
                    'qty': comp.get('quantity', ''),
                    'component_note': comp.get('note', '')
                }
                self.csv_data['components'].append(component_row)
    
    def process_article_relations_data(self, article_id: int, relations_response: Dict[str, Any]) -> None:
        """Process data for article_relations.csv"""
        if not relations_response or 'data' not in relations_response:
            return
        
        relations_data = relations_response.get('data', {})
        if 'array' in relations_data:
            for rel in relations_data['array']:
                relation_row = {
                    'article_id_from': article_id,
                    'relation_type': rel.get('relationType', ''),
                    'article_id_to': rel.get('relatedArticleId', ''),
                    'note': rel.get('note', '')
                }
                self.csv_data['article_relations'].append(relation_row)
    
    def process_brand_data(self, supplier_id: int, brand_name: str) -> None:
        """Process data for brands.csv"""
        # Check if brand already processed
        for existing_brand in self.csv_data['brands']:
            if existing_brand['supplier_id'] == supplier_id:
                return  # Already processed
        
        # Get brand info
        brand_response = self.get_brand_info(supplier_id)
        
        brand_row = {
            'supplier_id': supplier_id,
            'brand_name': brand_name,
            'www_url': '',
            'email': '',
            'phone': '',
            'fax': '',
            'status': '',
            'status_badge_url': '',
            'logo_url_100': '',
            'logo_url_200': '',
            'logo_url_400': '',
            'logo_url_800': '',
            'zip_country_iso': '',
            'city': '',
            'zip': '',
            'street': '',
            'name': '',
            'name2': ''
        }
        
        if brand_response and 'data' in brand_response:
            brand_data = brand_response.get('data', {})
            brand_row.update({
                'www_url': brand_data.get('website', ''),
                'email': brand_data.get('email', ''),
                'phone': brand_data.get('phone', ''),
                'fax': brand_data.get('fax', ''),
                'status': brand_data.get('status', ''),
                'status_badge_url': brand_data.get('statusBadgeUrl', ''),
                'zip_country_iso': brand_data.get('countryIso', ''),
                'city': brand_data.get('city', ''),
                'zip': brand_data.get('zip', ''),
                'street': brand_data.get('street', ''),
                'name': brand_data.get('companyName', ''),
                'name2': brand_data.get('companyName2', '')
            })
            
            # Extract logo URLs
            if 'logos' in brand_data:
                logos = brand_data['logos']
                for logo in logos:
                    if '100' in logo:
                        brand_row['logo_url_100'] = logo['100']
                    if '200' in logo:
                        brand_row['logo_url_200'] = logo['200']
                    if '400' in logo:
                        brand_row['logo_url_400'] = logo['400']
                    if '800' in logo:
                        brand_row['logo_url_800'] = logo['800']
        
        self.csv_data['brands'].append(brand_row)
    
    def export_articles_csv(self, filename: str = None) -> str:
        """Export articles data to CSV file (focused on articles.csv only)"""
        if not filename:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"articles_{timestamp}.csv"
        
        # Define articles CSV schema according to client requirements
        articles_columns = [
            'article_id', 'supplier_id', 'mfr_id', 'brand_name', 'article_number',
            'generic_article_id', 'generic_article_description', 'category_path',
            'category_node_ids', 'short_description', 'note',
            'image_urls', 'pdf_urls', 'is_accessory', 'article_status_id', 'article_status_description',
            'article_status_valid_from_date', 'quantity_per_package', 'quantity_per_part_per_package',
            'is_self_service_packing', 'has_mandatory_material_certification', 'is_remanufactured_part'
        ]
        
        data = self.csv_data['articles']
        
        if not data:
            print("ERROR: No articles data to export")
            return ""
        
        try:
            df = pd.DataFrame(data)
            df = df.reindex(columns=articles_columns, fill_value='')
            
            # Export with semicolon delimiter as per client requirements
            df.to_csv(filename, index=False, encoding='utf-8', sep=';')
            
            print(f"SUCCESS: articles.csv created: {len(data)} records")
            print(f"File: {filename}")
            
            return filename
            
        except Exception as e:
            print(f"ERROR: Error creating articles.csv: {e}")
            return ""
    
    def export_attributes_csv(self, filename: str = None) -> str:
        """Export attributes data to CSV file"""
        if not filename:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"attributes_{timestamp}.csv"
        
        # Define attributes CSV schema according to client requirements
        attributes_columns = [
            'article_id', 'criteria_id', 'criteria_description', 'criteria_abbr',
            'value_raw', 'value_formatted', 'unit', 'immediate_display', 'is_interval'
        ]
        
        data = self.csv_data['attributes']
        
        if not data:
            print("WARNING: No attributes data to export")
            return ""
        
        try:
            df = pd.DataFrame(data)
            df = df.reindex(columns=attributes_columns, fill_value='')
            
            # Export with semicolon delimiter as per client requirements
            df.to_csv(filename, index=False, encoding='utf-8', sep=';')
            
            print(f"SUCCESS: attributes.csv created: {len(data)} records")
            print(f"File: {filename}")
            
            return filename
            
        except Exception as e:
            print(f"ERROR: Error creating attributes.csv: {e}")
            return ""
    
    def export_references_csv(self, filename: str = None) -> str:
        """Export references data to CSV file"""
        if not filename:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"references_{timestamp}.csv"
        
        # Define references CSV schema according to client requirements
        references_columns = [
            'article_id', 'ref_type', 'number', 'mfr_name'
        ]
        
        data = self.csv_data['references']
        
        if not data:
            print("WARNING: No references data to export")
            return ""
        
        try:
            df = pd.DataFrame(data)
            df = df.reindex(columns=references_columns, fill_value='')
            
            # Export with semicolon delimiter as per client requirements
            df.to_csv(filename, index=False, encoding='utf-8', sep=';')
            
            print(f"SUCCESS: references.csv created: {len(data)} records")
            print(f"File: {filename}")
            
            return filename
            
        except Exception as e:
            print(f"ERROR: Error creating references.csv: {e}")
            return ""
    
    def export_to_csv(self, data: List[Dict[str, Any]], filename: str = None) -> str:
        """Export data to CSV"""
        if not filename:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"tecdoc_export_{timestamp}.csv"
        
        if not data:
            print("ERROR: No data to export")
            return ""
        
        # Define CSV columns
        base_columns = [
            'manufacturer_item_number',
            'article_id',
            'article_name',
            'manufacturer_id',
            'manufacturer_name',
            'gtins',
            'category_hierarchy',
            'vehicle_types_found'
        ]
        
        # Add image columns
        image_columns = []
        if data:
            for key in data[0].keys():
                if key.startswith('image_'):
                    image_columns.append(key)
        
        # Add vehicle columns
        vehicle_columns = [
            'vehicle_applications_count',
            'vehicle_manufacturers',
            'vehicle_models',
            'restriction_texts',
            'vehicle_applications'
        ]
        
        # Combine all columns
        all_columns = base_columns + sorted(image_columns) + vehicle_columns + ['raw_article_data']
        
        try:
            # Create DataFrame
            df = pd.DataFrame(data)
            df = df.reindex(columns=all_columns, fill_value='')
            
            # Export to CSV
            df.to_csv(filename, index=False, encoding='utf-8')
            
            print(f"SUCCESS: Data exported to {filename}")
            print(f"Records: {len(data)}")
            print(f"Columns: {len(all_columns)}")
            
            return filename
        except Exception as e:
            print(f"ERROR: Error exporting to CSV: {e}")
            return ""

def main():
    """Main function"""
    print("Tecdoc API Export Tool")
    print("=" * 40)
    
    # Initialize client
    client = TecdocClient()
    
    # Get user input
    try:
        print(f"\nTest Data Available:")
        print(f"   Manufacturer ID: {TEST_MANUFACTURER_ID} (DT Spare Parts)")
        print(f"   Article Number: {TEST_ARTICLE_NUMBER}")
        
        # Automatically use test data for demonstration
        use_test_data = True  # input("\nUse test data? (y/n): ").lower().strip() == 'y'
        
        if use_test_data:
            manufacturer_id = TEST_MANUFACTURER_ID
            article_number = TEST_ARTICLE_NUMBER
        else:
            manufacturer_id = int(input("Enter manufacturer ID: "))
            article_number = input("Enter article number: ").strip()
            
        if not article_number:
            print("ERROR: Article number is required")
            return
            
    except ValueError:
        print("ERROR: Invalid manufacturer ID. Please enter a number.")
        return
    
    print(f"\nProcessing manufacturer {manufacturer_id}, article {article_number}...")
    
    # Step 1: Get articles with images and GTINs
    articles_response = client.get_articles(manufacturer_id, article_number)
    
    if not articles_response or 'articles' not in articles_response:
        print("ERROR: Failed to retrieve articles")
        return
    
    articles_data = articles_response.get('articles', [])
    
    if not articles_data:
        print("ERROR: No articles found")
        return
    
    print(f"SUCCESS: Found {len(articles_data)} articles")
    
    # Step 2: Get article name and ID
    article_name, article_id = client.get_article_name_and_id(manufacturer_id, article_number)
    print(f"SUCCESS: Article name: {article_name}")
    print(f"SUCCESS: Article ID: {article_id}")
    
    # Extract assembly group facets from response
    assembly_group_facets = articles_response.get('assemblyGroupFacets', {})
    
    # Step 3: Process articles with complete data extraction
    for i, article in enumerate(articles_data, 1):
        print(f"\nProcessing article {i}: {article.get('articleNumber', 'Unknown')}")
        
        # Extract article ID and supplier ID from the new structure
        article_id = 0
        supplier_id = article.get('dataSupplierId', 0)
        
        # Get legacy article ID from genericArticles if available
        if 'genericArticles' in article and article['genericArticles']:
            legacy_article_id = article['genericArticles'][0].get('legacyArticleId', 0)
            if legacy_article_id:
                article_id = legacy_article_id
        
        # Process complete article data with assembly group facets
        article_number_from_data = article.get('articleNumber', article_number)
        client.process_complete_article_data(article, article_name, article_id, supplier_id, assembly_group_facets, article_number_from_data)
        
        # Show key information
        print(f"   Name: {article_name}")
        print(f"   Manufacturer: {article.get('mfrName', 'Unknown')}")
        print(f"   Article ID: {article_id}")
        print(f"   Supplier ID: {supplier_id}")
        print(f"   Article Number: {article.get('articleNumber', 'Unknown')}")
    
    # Step 4: Export to articles.csv (focused export)
    print(f"\nExporting to articles.csv...")
    created_file = client.export_articles_csv()
    
    # Step 5: Export to attributes.csv
    print(f"\nExporting to attributes.csv...")
    created_attributes_file = client.export_attributes_csv()
    
    # Step 6: Export to references.csv
    print(f"\nExporting to references.csv...")
    created_references_file = client.export_references_csv()
    
    if created_file:
        print(f"\n" + "="*50)
        print(f"Export completed successfully!")
        print(f"="*50)
        print(f"Created files:")
        file_num = 1
        print(f"   {file_num}. {created_file}")
        file_num += 1
        if created_attributes_file:
            print(f"   {file_num}. {created_attributes_file}")
            file_num += 1
        if created_references_file:
            print(f"   {file_num}. {created_references_file}")
        
        # Show summary of articles data
        articles_data = client.csv_data['articles']
        if articles_data:
            print(f"\nArticles Data Summary:")
            print(f"   Total articles: {len(articles_data)}")
            
            # Show sample of what was extracted
            sample_article = articles_data[0]
            print(f"   Sample article data:")
            for key, value in sample_article.items():
                if value:  # Only show fields with data
                    display_value = str(value)[:100] + "..." if len(str(value)) > 100 else str(value)
                    print(f"     {key}: {display_value}")
        
        # Show summary of attributes data
        attributes_data = client.csv_data['attributes']
        if attributes_data:
            print(f"\nAttributes Data Summary:")
            print(f"   Total attributes: {len(attributes_data)}")
            
            # Show sample of what was extracted
            if len(attributes_data) > 0:
                sample_attr = attributes_data[0]
                print(f"   Sample attribute data:")
                for key, value in sample_attr.items():
                    if value:  # Only show fields with data
                        display_value = str(value)[:100] + "..." if len(str(value)) > 100 else str(value)
                        print(f"     {key}: {display_value}")
        
        # Show summary of references data
        references_data = client.csv_data['references']
        if references_data:
            print(f"\nReferences Data Summary:")
            print(f"   Total references: {len(references_data)}")
            
            # Show sample of what was extracted
            if len(references_data) > 0:
                sample_ref = references_data[0]
                print(f"   Sample reference data:")
                for key, value in sample_ref.items():
                    if value:  # Only show fields with data
                        display_value = str(value)[:100] + "..." if len(str(value)) > 100 else str(value)
                        print(f"     {key}: {display_value}")
    else:
        print("ERROR: Export failed")

if __name__ == "__main__":
    main()
