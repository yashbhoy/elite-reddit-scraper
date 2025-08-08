#!/usr/bin/env python3
"""
ELITE Reddit Influencer Scraper with Multi-Key ScraperAPI Support - OPTIMIZED
Supports multiple ScraperAPI keys with intelligent rotation and credit management.
Designed for GitHub Actions with free plan credit optimization.
COMPLETE VERSION - All original functionality preserved and enhanced.
"""
import requests
import json
import time
import csv
import random
from datetime import datetime, timedelta
from typing import List, Dict, Set, Union, Optional, Any
import os
import logging
from pathlib import Path
import itertools
from urllib.parse import urlencode

class MultiKeyConfig:
    """Configuration for multiple ScraperAPI keys with smart rotation."""
    
    def __init__(self):
        # Load multiple API keys from environment
        self.api_keys = self._load_api_keys()
        self.key_stats = {key: {'requests': 0, 'errors': 0, 'last_error_time': 0, 'blocked': False, 'credits_used': 0} 
                         for key in self.api_keys}
        self.current_key_index = 0
        self.max_requests_per_key = 4500  # Conservative for free plan (1000 credits)
        self.max_errors_per_key = 5  # Block key after consecutive errors
        self.error_reset_time = 3600  # Reset error count after 1 hour
        
    def _load_api_keys(self) -> List[str]:
        """Load multiple API keys from environment variables."""
        keys = []
        
        # Primary key
        primary_key = os.getenv('SCRAPE_KEY') or os.getenv('SCRAPERAPI_KEY')
        if primary_key:
            keys.append(primary_key)
        
        # Additional keys (SCRAPERAPI_KEY_2, SCRAPERAPI_KEY_3, etc.)
        key_index = 2
        while True:
            additional_key = os.getenv(f'SCRAPERAPI_KEY_{key_index}')
            if additional_key:
                keys.append(additional_key)
                key_index += 1
            else:
                break
        
        # Fallback keys from comma-separated string
        keys_string = os.getenv('SCRAPERAPI_KEYS')
        if keys_string:
            fallback_keys = [key.strip() for key in keys_string.split(',') if key.strip()]
            keys.extend(fallback_keys)
        
        # Built-in fallback key (your original)
        if not keys:
            keys.append('001dfb055d3443ea6a8ba1e0d2ac3562')
        
        # Remove duplicates while preserving order
        unique_keys = []
        for key in keys:
            if key not in unique_keys:
                unique_keys.append(key)
        
        return unique_keys
    
    def get_active_key(self) -> Optional[str]:
        """Get the current active API key with smart rotation."""
        if not self.api_keys:
            return None
        
        # Try to find a non-blocked key
        attempts = 0
        while attempts < len(self.api_keys):
            current_key = self.api_keys[self.current_key_index]
            stats = self.key_stats[current_key]
            
            # Reset error count if enough time has passed
            if stats['errors'] > 0 and time.time() - stats['last_error_time'] > self.error_reset_time:
                stats['errors'] = 0
                stats['blocked'] = False
            
            # Check if key is usable
            if (not stats['blocked'] and 
                stats['requests'] < self.max_requests_per_key and 
                stats['errors'] < self.max_errors_per_key):
                return current_key
            
            # Move to next key
            self.current_key_index = (self.current_key_index + 1) % len(self.api_keys)
            attempts += 1
        
        # All keys exhausted or blocked, return least used key
        available_keys = [(key, stats) for key, stats in self.key_stats.items() 
                         if stats['requests'] < self.max_requests_per_key]
        
        if available_keys:
            best_key = min(available_keys, key=lambda x: x[1]['requests'])[0]
            self.current_key_index = self.api_keys.index(best_key)
            return best_key
        
        return None
    
    def record_request(self, api_key: str, success: bool = True, error_type: Optional[str] = None):
        """Record API request stats for intelligent rotation."""
        if api_key in self.key_stats:
            stats = self.key_stats[api_key]
            stats['requests'] += 1
            stats['credits_used'] += 1  # Assume 1 credit per request
            
            if not success:
                stats['errors'] += 1
                stats['last_error_time'] = time.time()
                
                # Block key if too many errors or specific error types
                if (stats['errors'] >= self.max_errors_per_key or 
                    error_type in ['403', '401', 'quota_exceeded']):
                    stats['blocked'] = True
            else:
                # Reset error count on successful request
                if stats['errors'] > 0:
                    stats['errors'] = max(0, stats['errors'] - 1)
    
    def get_stats_summary(self) -> Dict[str, Any]:
        """Get summary of all key statistics."""
        total_requests = sum(stats['requests'] for stats in self.key_stats.values())
        total_credits = sum(stats['credits_used'] for stats in self.key_stats.values())
        active_keys = sum(1 for stats in self.key_stats.values() if not stats['blocked'])
        
        return {
            'total_keys': len(self.api_keys),
            'active_keys': active_keys,
            'total_requests': total_requests,
            'total_credits_used': total_credits,
            'key_details': {
                f"Key_{i+1}": {
                    'requests': stats['requests'],
                    'credits': stats['credits_used'],
                    'errors': stats['errors'],
                    'blocked': stats['blocked'],
                    'last_8_chars': key[-8:] if len(key) >= 8 else key
                }
                for i, (key, stats) in enumerate(self.key_stats.items())
            }
        }

class EliteRedditScraperMultiKey:
    """Elite scraper with multi-key ScraperAPI support and smart credit management."""
    
    def __init__(self, target_count: int = 2500) -> None:
        self.target_count = target_count
        self.influencers: List[Dict[str, Any]] = []
        self.scraped_users: Set[str] = set()
        self.failed_users: Set[str] = set()
        
        # Multi-key configuration
        self.multi_key_config = MultiKeyConfig()
        self.gateway_enabled = len(self.multi_key_config.api_keys) > 0
        
        # Setup logging first
        self._setup_logging()
        
        if not self.gateway_enabled:
            self.logger.error("❌ No ScraperAPI keys found! Add SCRAPERAPI_KEY or SCRAPERAPI_KEY_2, etc.")
            raise ValueError("No API keys available")
        else:
            stats = self.multi_key_config.get_stats_summary()
            self.logger.info(f"🛡️ Multi-Key ScraperAPI enabled: {stats['total_keys']} keys loaded")
            for i, key in enumerate(self.multi_key_config.api_keys):
                self.logger.info(f"  Key {i+1}: ...{key[-8:]} (Ready)")
        
        # Progress tracking
        self.progress_file = 'elite_scraper_progress.json'
        self.csv_file = 'reddit_elite_influencers.csv'
        self.backup_interval = 50
        
        # OPTIMIZED rate limiting for multi-key setup
        self.request_count = 0
        self.session_start = time.time()
        self.base_delay = 0.2  # Faster with multiple keys
        self.current_delay = self.base_delay
        
        # Session management - optimized
        self.sessions = self._create_sessions()
        self.current_session_idx = 0
        
        # Enhanced subreddit list - preserved from original
        self.subreddit_categories = {
            'crypto_defi_blockchain': [
                'CryptoCurrency', 'Bitcoin', 'Ethereum', 'CryptoMarkets', 'defi', 'altcoin', 
                'CryptoTechnology', 'CryptoMoonShots', 'SatoshiStreetBets', 'ethtrader',
                'dogecoin', 'cardano', 'solana', 'NFT', 'NFTs', 'web3', 'binance', 'BitcoinBeginners'
            ],
            'ai_machine_learning_tech': [
                'artificial', 'MachineLearning', 'ChatGPT', 'singularity', 'OpenAI',
                'ArtificialIntelligence', 'LocalLLaMA', 'datascience', 'dataengineering',
                'analytics', 'statistics', 'MachineLearningNews', 'programming', 'python',
                'javascript', 'webdev', 'learnprogramming', 'cybersecurity', 'devops',
                'aws', 'docker', 'kubernetes', 'linux', 'StableDiffusion', 'midjourney', 'ComputerVision'
            ],
            'investing_finance_business': [
                'personalfinance', 'wallstreetbets', 'investing', 'stocks', 'StockMarket',
                'financialindependence', 'SecurityAnalysis', 'options', 'pennystocks',
                'dividendinvesting', 'ValueInvesting', 'RobinHood', 'Entrepreneur', 'startups',
                'smallbusiness', 'business', 'GrowthHacking', 'SaaS', 'growmybusiness',
                'realestate', 'Economics', 'finance', 'FinancialCareers', 'frugal',
                'tax', 'retirement', 'creditcards', 'financialplanning'
            ],
            'content_creator_influencer': [
                'NewTubers', 'youtube', 'tiktok', 'instagram', 'streaming', 'twitch',
                'letsplay', 'podcasting', 'blogging', 'copywriting', 'VideoEditing',
                'ContentCreation', 'YouTube_startups', 'marketing', 'DigitalMarketing',
                'SEO', 'PPC', 'advertising', 'Affiliatemarketing', 'content_marketing',
                'socialmedia', 'branding', 'influencer'
            ],
            'wellness_fitness_lifestyle': [
                'fitness', 'loseit', 'gainit', 'bodyweightfitness', 'xxfitness', 'running',
                'weightlifting', 'yoga', 'nutrition', 'MealPrepSunday', 'EatCheapAndHealthy',
                'intermittentfasting', 'keto', 'mentalhealth', 'anxiety', 'depression',
                'therapy', 'psychology', 'meditation', 'mindfulness', 'getmotivated',
                'selfimprovement', 'productivity', 'getdisciplined', 'decidingtobebetter'
            ],
            'massive_general_engagement': [
                'AskReddit', 'todayilearned', 'explainlikeimfive', 'LifeProTips', 'Showerthoughts',
                'mildlyinteresting', 'interestingasfuck', 'Damnthatsinteresting', 'MadeMeSmile',
                'unpopularopinion', 'news', 'worldnews', 'science', 'dataisbeautiful',
                'nottheonion', 'videos', 'gifs', 'pics', 'funny', 'memes', 'gaming',
                'movies', 'music', 'aww', 'IAmA'
            ],
            'creative_arts_entertainment': [
                'art', 'photography', 'design', 'graphic_design', 'UI_UX', 'photoshop',
                'blender', 'animation', 'writing', 'screenwriting', 'books', 'MovieSuggestions',
                'movies', 'television', 'Music', 'WeAreTheMusicMakers', 'edmproduction',
                'trapproduction', 'makinghiphop', 'gamedev', 'IndieGameDev', 'gamedesign'
            ],
            'fashion_travel_culture': [
                'malefashionadvice', 'femalefashionadvice', 'streetwear', 'sneakers',
                'frugalmalefashion', 'frugalfemalefashion', 'buyitforlife', 'travel',
                'solotravel', 'backpacking', 'digitalnomad', 'onebag', 'shoestring',
                'travel_tips', 'roadtrip', 'churning', 'awardtravel', 'camping',
                'hiking', 'outdoors'
            ]
        }
        
        self.minimum_karma = 50000
        self.tier_priorities = ['MEGA', 'SUPER', 'MAJOR']
        
    def _setup_logging(self) -> None:
        """Setup comprehensive logging."""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler('elite_scraper.log'),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)
    
    def _create_sessions(self) -> List[requests.Session]:
        """Create multiple sessions with realistic browser headers."""
        user_agents = [
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2.1 Safari/605.1.15',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:123.0) Gecko/20100101 Firefox/123.0',
            'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Edge/122.0.0.0 Safari/537.36'
        ]
        
        sessions = []
        for ua in user_agents:
            session = requests.Session()
            
            session.headers.update({
                'User-Agent': ua,
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.9',
                'Accept-Encoding': 'gzip, deflate, br',
                'DNT': '1',
                'Connection': 'keep-alive',
                'Upgrade-Insecure-Requests': '1',
            })
            
            # Configure retries - optimized for multi-key setup
            try:
                from requests.adapters import HTTPAdapter
                from urllib3.util.retry import Retry
                
                retry_strategy = Retry(
                    total=2,  # Reduced for faster key rotation
                    backoff_factor=1.0,
                    status_forcelist=[403, 429, 500, 502, 503, 504],
                )
                adapter = HTTPAdapter(max_retries=retry_strategy)
                session.mount("http://", adapter)
                session.mount("https://", adapter)
            except ImportError:
                pass
            
            sessions.append(session)
            
        return sessions
    
    def _multi_key_scraperapi_request(self, url: str, params: Optional[Dict] = None, **kwargs) -> requests.Response:
        """Make request using multi-key ScraperAPI with intelligent key rotation."""
        if params:
            query_string = urlencode(params)
            target_url = f"{url}?{query_string}"
        else:
            target_url = url
        
        max_key_attempts = len(self.multi_key_config.api_keys)
        attempt = 0
        
        while attempt < max_key_attempts:
            # Get active API key
            api_key = self.multi_key_config.get_active_key()
            if not api_key:
                # All keys exhausted
                self.logger.error("❌ All API keys exhausted or blocked!")
                # Wait and retry with least used key
                time.sleep(60)
                stats = self.multi_key_config.get_stats_summary()
                self.logger.info("⏳ Attempting with least used key after cooldown...")
                api_key = min(self.multi_key_config.api_keys, 
                             key=lambda k: self.multi_key_config.key_stats[k]['requests'])
            
            # Build ScraperAPI request
            gateway_params = {
                'api_key': api_key,
                'url': target_url,
                'render': 'false',
                'country_code': 'US',
                'premium': 'false'  # Free plan optimization
            }
            
            session = self._get_session()
            kwargs_copy = kwargs.copy()
            kwargs_copy.pop('params', None)
            kwargs_copy['timeout'] = kwargs_copy.get('timeout', 25)
            
            try:
                response = session.get('http://api.scraperapi.com', params=gateway_params, **kwargs_copy)
                
                if response.status_code == 200:
                    # Success - record and return
                    self.multi_key_config.record_request(api_key, success=True)
                    return response
                elif response.status_code == 401:
                    # Invalid API key
                    self.multi_key_config.record_request(api_key, success=False, error_type='401')
                    self.logger.warning(f"⚠️ Invalid API key ...{api_key[-8:]}, rotating to next key")
                elif response.status_code == 403:
                    # Quota exceeded or blocked
                    self.multi_key_config.record_request(api_key, success=False, error_type='403')
                    self.logger.warning(f"⚠️ API key ...{api_key[-8:]} quota exceeded, rotating to next key")
                elif response.status_code == 429:
                    # Rate limited
                    self.multi_key_config.record_request(api_key, success=False, error_type='429')
                    self.logger.warning(f"⚠️ Rate limited on key ...{api_key[-8:]}, rotating to next key")
                    time.sleep(random.uniform(2, 5))
                else:
                    # Other error
                    self.multi_key_config.record_request(api_key, success=False, error_type=str(response.status_code))
                    self.logger.warning(f"⚠️ Error {response.status_code} with key ...{api_key[-8:]}")
                
                # Try next key
                attempt += 1
                continue
                
            except requests.exceptions.RequestException as e:
                self.multi_key_config.record_request(api_key, success=False, error_type='connection_error')
                self.logger.warning(f"⚠️ Connection error with key ...{api_key[-8:]}: {e}")
                attempt += 1
                time.sleep(random.uniform(1, 3))
                continue
        
        # If we get here, all keys failed
        raise requests.exceptions.RequestException("All API keys failed or exhausted")
    
    def _get_session(self) -> requests.Session:
        """Get current session and rotate if needed."""
        session = self.sessions[self.current_session_idx]
        
        # Rotate sessions more frequently for better distribution
        if self.request_count % 150 == 0 and self.request_count > 0:
            self.current_session_idx = (self.current_session_idx + 1) % len(self.sessions)
            
        return session
    
    def _adaptive_delay(self) -> None:
        """Optimized adaptive delay for multi-key setup."""
        self.request_count += 1
        
        # Shorter delays with multiple keys
        base_delay = 0.1 if len(self.multi_key_config.api_keys) > 3 else 0.2
        
        # Add small jitter
        jitter = random.uniform(0.5, 1.5)
        actual_delay = base_delay * jitter
        
        time.sleep(actual_delay)
        
        # Log key rotation stats periodically
        if self.request_count % 100 == 0:
            stats = self.multi_key_config.get_stats_summary()
            self.logger.info(f"🔑 Multi-Key Stats: {stats['active_keys']}/{stats['total_keys']} active, "
                           f"{stats['total_credits_used']} credits used")
    
    def _handle_rate_limit(self, response: requests.Response) -> bool:
        """Handle rate limiting with multi-key awareness."""
        if response.status_code == 429:
            self.logger.warning("⚠️ Rate limited! Rotating to next key...")
            # Short delay then let key rotation handle it
            time.sleep(random.uniform(1, 3))
            return True
        elif response.status_code in [502, 503, 504]:
            self.logger.warning(f"🔧 Server error {response.status_code}, trying next key...")
            time.sleep(random.uniform(2, 5))
            return True
        return False
    
    def _save_progress(self) -> None:
        """Save current progress with multi-key stats."""
        progress_data = {
            'total_scraped': len(self.influencers),
            'scraped_users': list(self.scraped_users),
            'failed_users': list(self.failed_users),
            'timestamp': datetime.now().isoformat(),
            'request_count': self.request_count,
            'multi_key_stats': self.multi_key_config.get_stats_summary()
        }
        
        with open(self.progress_file, 'w') as f:
            json.dump(progress_data, f)
        
        if self.influencers:
            self._save_to_csv(f'backup_{self.csv_file}')
    
    def _load_progress(self) -> bool:
        """Load previous progress if exists."""
        if os.path.exists(self.progress_file):
            try:
                with open(self.progress_file, 'r') as f:
                    progress_data = json.load(f)
                
                self.scraped_users = set(progress_data.get('scraped_users', []))
                self.failed_users = set(progress_data.get('failed_users', []))
                self.request_count = progress_data.get('request_count', 0)
                
                # Load previous key stats if available
                if 'multi_key_stats' in progress_data:
                    prev_stats = progress_data['multi_key_stats']
                    self.logger.info(f"📊 Previous session used {prev_stats.get('total_credits_used', 0)} credits")
                
                if os.path.exists(self.csv_file):
                    self._load_existing_csv()
                
                self.logger.info(f"🔄 Resumed from progress: {len(self.influencers)} elite influencers loaded")
                return True
            except Exception as e:
                self.logger.error(f"❌ Failed to load progress: {e}")
        return False
    
    def _load_existing_csv(self) -> None:
        """Load existing CSV data."""
        try:
            with open(self.csv_file, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    # Convert numeric fields
                    for field in ['total_karma', 'link_karma', 'comment_karma', 'account_age_days', 'estimated_reach']:
                        if field in row and row[field]:
                            row[field] = int(row[field])
                    
                    if 'karma_per_day' in row and row['karma_per_day']:
                        row['karma_per_day'] = float(row['karma_per_day'])
                    
                    for field in ['is_verified', 'has_premium', 'has_verified_email']:
                        if field in row and row[field]:
                            row[field] = row[field].lower() == 'true'
                    
                    self.influencers.append(row)
        except Exception as e:
            self.logger.error(f"❌ Failed to load existing CSV: {e}")
    
    def get_elite_posts_only(self, subreddit: str, limit: int = 100, max_retries: int = 2) -> List[Dict[str, Any]]:
        """Get only TOP and HOT posts using multi-key ScraperAPI."""
        all_posts = []
        
        # Streamlined sort configs for speed
        sort_configs = [
            ('hot', None),      # Currently trending
            ('top', 'week'),    # Best of the week
            ('top', 'month'),   # Best of the month
        ]
        
        for sort_type, time_filter in sort_configs:
            url = f"https://www.reddit.com/r/{subreddit}/{sort_type}.json"
            params: Dict[str, Union[str, int]] = {'limit': limit}
            
            if sort_type == 'top' and time_filter is not None:
                params['t'] = time_filter
            
            for attempt in range(max_retries):
                try:
                    response = self._multi_key_scraperapi_request(url, params=params)
                    
                    if self._handle_rate_limit(response):
                        continue
                        
                    response.raise_for_status()
                    data = response.json()
                    posts = data.get('data', {}).get('children', [])
                    
                    all_posts.extend(posts)
                    self._adaptive_delay()
                    break
                    
                except Exception as e:
                    self.logger.warning(f"⚠️ Attempt {attempt + 1} failed for r/{subreddit} ({sort_type}): {e}")
                    if attempt < max_retries - 1:
                        time.sleep(random.uniform(2, 6))
                    else:
                        self.logger.error(f"❌ Failed to get {sort_type} posts from r/{subreddit}")
                        continue
        
        return all_posts
    
    def get_user_with_retry(self, username: str, max_retries: int = 2) -> Dict[str, Any]:
        """Get user data with multi-key retry logic."""
        if username in self.scraped_users or username in self.failed_users:
            return {}
        
        url = f"https://www.reddit.com/user/{username}/about.json"
        
        for attempt in range(max_retries):
            try:
                response = self._multi_key_scraperapi_request(url)
                
                if self._handle_rate_limit(response):
                    continue
                
                if response.status_code == 404:
                    self.failed_users.add(username)
                    return {}
                
                response.raise_for_status()
                data = response.json()
                user_data = data.get('data', {})
                
                if user_data:
                    self.scraped_users.add(username)
                    self._adaptive_delay()
                    return user_data
                else:
                    self.failed_users.add(username)
                    return {}
                
            except Exception as e:
                self.logger.warning(f"⚠️ Attempt {attempt + 1} failed for user {username}: {e}")
                if attempt < max_retries - 1:
                    time.sleep(random.uniform(2, 6))
                else:
                    self.logger.error(f"❌ Failed to get user {username} after {max_retries} attempts")
                    self.failed_users.add(username)
                    return {}
        
        return {}
    
    def analyze_elite_user(self, user_data: Dict[str, Any]) -> Dict[str, Any]:
        """ELITE USER ANALYSIS - Only accept high-karma accounts."""
        if not user_data:
            return {}
        
        username = user_data.get('name', '')
        link_karma = user_data.get('link_karma', 0)
        comment_karma = user_data.get('comment_karma', 0)
        total_karma = link_karma + comment_karma
        
        # STRICT ELITE FILTERING
        if total_karma < self.minimum_karma:
            return {}
        
        if user_data.get('is_suspended', False):
            return {}
        
        if username in ['[deleted]', '', None]:
            return {}
        
        account_age_days = (datetime.now().timestamp() - user_data.get('created_utc', 0)) / 86400
        
        if account_age_days < 90:
            return {}
        
        karma_per_day = total_karma / max(account_age_days, 1)
        
        # ELITE TIER CLASSIFICATION
        if total_karma >= 1000000:
            tier = 'MEGA'
            reach_multiplier = 4.0
        elif total_karma >= 250000:
            tier = 'SUPER' 
            reach_multiplier = 3.0
        elif total_karma >= 50000:
            tier = 'MAJOR'
            reach_multiplier = 2.5
        else:
            return {}
        
        # Additional quality checks
        if karma_per_day > 10000:  # Bot check
            return {}
        
        if comment_karma == 0 and link_karma > 100000:  # Spammer check
            return {}
        
        estimated_reach = int(total_karma * reach_multiplier)
        
        # Generate Reddit profile link
        reddit_profile_url = f"https://www.reddit.com/user/{username}"
        
        influencer = {
            'username': username,
            'reddit_profile_url': reddit_profile_url,
            'total_karma': total_karma,
            'link_karma': link_karma,
            'comment_karma': comment_karma,
            'account_age_days': int(account_age_days),
            'karma_per_day': round(karma_per_day, 2),
            'tier': tier,
            'estimated_reach': estimated_reach,
            'is_verified': user_data.get('is_employee', False),
            'has_premium': user_data.get('is_gold', False),
            'has_verified_email': user_data.get('has_verified_email', False),
            'scraped_at': datetime.now().isoformat()
        }
        
        return influencer
    
    def scrape_subreddit_elite(self, subreddit: str, target_users: int = 40) -> int:
        """Optimized elite subreddit scraping with multi-key support."""
        self.logger.info(f"🎯 ELITE scraping r/{subreddit} (targeting {target_users} top users)")
        
        found_users = 0
        posts = self.get_elite_posts_only(subreddit, 100)
        
        # Sort posts by score to prioritize high-engagement content
        posts.sort(key=lambda x: x.get('data', {}).get('score', 0), reverse=True)
        
        for post_data in posts:
            if found_users >= target_users or len(self.influencers) >= self.target_count:
                break
            
            # Check if we're approaching credit limits
            stats = self.multi_key_config.get_stats_summary()
            if stats['total_credits_used'] > (stats['total_keys'] * 4500):  # 90% of credits used
                self.logger.warning("⚠️ Approaching credit limits, saving progress...")
                self._save_progress()
            
            post = post_data.get('data', {})
            username = post.get('author', '')
            post_score = post.get('score', 0)
            
            # Lower threshold for faster results but still quality-focused
            if post_score < 500:
                continue
            
            if (username and username != '[deleted]' and 
                username not in self.scraped_users and 
                username not in self.failed_users):
                
                user_data = self.get_user_with_retry(username)
                influencer = self.analyze_elite_user(user_data)
                
                if influencer:
                    self.influencers.append(influencer)
                    found_users += 1
                    
                    self.logger.info(f"🏆 Added u/{username} ({influencer['tier']}, {influencer['total_karma']:,} karma)")
                    
                    if len(self.influencers) % self.backup_interval == 0:
                        self._save_progress()
                        self.logger.info(f"💾 Progress saved: {len(self.influencers)}/{self.target_count}")
        
        self.logger.info(f"📊 Found {found_users} elite users from r/{subreddit}")
        return found_users
    
    def scrape_all_categories_elite(self) -> None:
        """Scrape all categories with multi-key credit management."""
        self.logger.info(f"🚀 MULTI-KEY ELITE SCRAPING - Targeting {self.target_count} top-tier influencers")
        self.logger.info("🏆 Minimum Karma: 50,000 (MAJOR tier and above)")
        self.logger.info("🎯 Tiers: MEGA (1M+), SUPER (250K+), MAJOR (50K+)")
        
        initial_stats = self.multi_key_config.get_stats_summary()
        self.logger.info(f"🔑 Starting with {initial_stats['total_keys']} API keys, "
                        f"{initial_stats['total_credits_used']} credits already used")
        
        category_stats: Dict[str, int] = {category: 0 for category in self.subreddit_categories.keys()}
        
        # Prioritize categories by potential for high-karma users
        priority_order = [
            'massive_general_engagement',    # Highest karma potential
            'crypto_defi_blockchain',        # Very active, high-karma community
            'investing_finance_business',    # High-value discussions
            'ai_machine_learning_tech',      # Growing, high-engagement
            'content_creator_influencer',    # Professional creators
            'wellness_fitness_lifestyle',    # Large, engaged communities
            'creative_arts_entertainment',   # Showcase communities
            'fashion_travel_culture'         # Lifestyle communities
        ]
        
        for category_name in priority_order:
            if len(self.influencers) >= self.target_count:
                break
                
            # Check credit usage before starting new category
            current_stats = self.multi_key_config.get_stats_summary()
            if current_stats['active_keys'] == 0:
                self.logger.error("❌ No active API keys remaining!")
                break
            elif current_stats['total_credits_used'] > (current_stats['total_keys'] * 4750):
                self.logger.warning("⚠️ 95% of credits used, stopping to preserve resources")
                break
                
            subreddits = self.subreddit_categories[category_name]
            self.logger.info(f"\n🎯 CATEGORY: {category_name.upper()} ({len(subreddits)} subreddits)")
            self.logger.info(f"🔑 Active keys: {current_stats['active_keys']}/{current_stats['total_keys']}, "
                           f"Credits used: {current_stats['total_credits_used']}")
            
            # Shuffle for variety but maintain quality focus
            random.shuffle(subreddits)
            
            # Adjust targets based on remaining credits
            remaining_credits = (current_stats['total_keys'] * 1000) - current_stats['total_credits_used']
            if remaining_credits < 500:
                users_per_subreddit = 15  # Conservative for low credits
            elif remaining_credits < 1000:
                users_per_subreddit = 20  # Moderate
            else:
                users_per_subreddit = 25  # Normal operation
            
            for subreddit in subreddits:
                if len(self.influencers) >= self.target_count:
                    break
                
                # Double-check credit status
                live_stats = self.multi_key_config.get_stats_summary()
                if live_stats['active_keys'] == 0:
                    self.logger.warning("⚠️ All keys exhausted during category processing")
                    break
                
                try:
                    added_count = self.scrape_subreddit_elite(subreddit, users_per_subreddit)
                    category_stats[category_name] += added_count
                    
                    # Progress logging every 100 accounts
                    if len(self.influencers) % 100 == 0 and len(self.influencers) > 0:
                        progress = (len(self.influencers) / self.target_count) * 100
                        live_stats = self.multi_key_config.get_stats_summary()
                        self.logger.info(f"📈 Progress: {len(self.influencers)}/{self.target_count} ({progress:.1f}%) "
                                       f"| Credits: {live_stats['total_credits_used']}")
                    
                except Exception as e:
                    self.logger.error(f"❌ Error scraping r/{subreddit}: {e}")
                    continue
                
                # Show tier distribution every 250 accounts
                if len(self.influencers) % 250 == 0 and len(self.influencers) > 0:
                    self._show_elite_distribution()
        
        # Final category breakdown
        final_stats = self.multi_key_config.get_stats_summary()
        self.logger.info(f"\n📊 FINAL CATEGORY BREAKDOWN:")
        for category, count in category_stats.items():
            percentage = (count / len(self.influencers)) * 100 if self.influencers else 0
            self.logger.info(f"  {category.upper():<30}: {count:>4,} accounts ({percentage:.1f}%)")
        
        self.logger.info(f"\n🔑 FINAL MULTI-KEY STATISTICS:")
        for key_name, key_data in final_stats['key_details'].items():
            self.logger.info(f"  {key_name}: {key_data['requests']} requests, "
                           f"{key_data['credits']} credits, "
                           f"{'BLOCKED' if key_data['blocked'] else 'ACTIVE'}")
    
    def _show_elite_distribution(self) -> None:
        """Show current elite tier distribution."""
        tier_counts: Dict[str, int] = {}
        total_karma = 0
        
        for influencer in self.influencers:
            tier = influencer['tier']
            tier_counts[tier] = tier_counts.get(tier, 0) + 1
            total_karma += influencer['total_karma']
        
        avg_karma = total_karma / len(self.influencers) if self.influencers else 0
        
        self.logger.info(f"🏆 ELITE TIER DISTRIBUTION (Avg: {avg_karma:,.0f} karma):")
        for tier in ['MEGA', 'SUPER', 'MAJOR']:
            count = tier_counts.get(tier, 0)
            if count > 0:
                percentage = (count / len(self.influencers)) * 100
                tier_karma = sum(i['total_karma'] for i in self.influencers if i['tier'] == tier)
                tier_avg = tier_karma / count
                self.logger.info(f"    {tier:<6}: {count:>4,} ({percentage:.1f}%) - Avg: {tier_avg:,.0f} karma")
    
    def _save_to_csv(self, filename: Optional[str] = None) -> None:
        """Save elite influencers to CSV with Reddit profile URLs."""
        if not self.influencers:
            self.logger.warning("⚠️ No elite influencers to save!")
            return
        
        filename = filename or self.csv_file
        
        # Updated fieldnames to include Reddit profile URL
        fieldnames = ['username', 'reddit_profile_url', 'tier', 'total_karma', 'link_karma', 'comment_karma',
                     'account_age_days', 'karma_per_day', 'estimated_reach', 
                     'is_verified', 'has_premium', 'has_verified_email', 'scraped_at']
        
        with open(filename, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            
            # Sort by karma before saving (highest first)
            sorted_influencers = sorted(self.influencers, key=lambda x: x['total_karma'], reverse=True)
            writer.writerows(sorted_influencers)
        
        self.logger.info(f"💾 Saved {len(self.influencers)} elite influencers to {filename}")
    
    def print_elite_summary(self) -> None:
        """Print comprehensive elite summary with multi-key stats."""
        if not self.influencers:
            self.logger.error("❌ No elite influencers found!")
            return
        
        tier_counts: Dict[str, int] = {}
        total_reach = 0
        total_karma = 0
        
        for influencer in self.influencers:
            tier = influencer['tier']
            tier_counts[tier] = tier_counts.get(tier, 0) + 1
            total_reach += influencer['estimated_reach']
            total_karma += influencer['total_karma']
        
        avg_karma = total_karma / len(self.influencers)
        final_stats = self.multi_key_config.get_stats_summary()
        
        print(f"\n🏆 MULTI-KEY ELITE SCRAPING RESULTS")
        print(f"{'='*70}")
        print(f"✅ Total Elite Influencers: {len(self.influencers):,}")
        print(f"🎯 Target Achievement: {(len(self.influencers)/self.target_count)*100:.1f}%")
        print(f"📊 Combined Karma: {total_karma:,}")
        print(f"📈 Average Karma: {avg_karma:,.0f}")
        print(f"📈 Estimated Total Reach: {total_reach:,}")
        print(f"⏱️  Total Requests: {self.request_count:,}")
        print(f"🕐 Runtime: {(time.time() - self.session_start)/3600:.1f} hours")
        print(f"🔥 Quality Focus: 50K+ karma minimum")
        print(f"🔗 Profile URLs: Included in CSV export")
        
        print(f"\n🔑 MULTI-KEY API STATISTICS:")
        print(f"  Total Keys Used: {final_stats['total_keys']}")
        print(f"  Active Keys: {final_stats['active_keys']}")
        print(f"  Total Credits Used: {final_stats['total_credits_used']}")
        print(f"  Credits per Account: {final_stats['total_credits_used']/len(self.influencers):.1f}")
        
        print(f"\n🔑 INDIVIDUAL KEY PERFORMANCE:")
        for key_name, key_data in final_stats['key_details'].items():
            status = "🔴 BLOCKED" if key_data['blocked'] else "🟢 ACTIVE"
            print(f"  {key_name} (...{key_data['last_8_chars']}): "
                  f"{key_data['requests']} requests, {key_data['credits']} credits, "
                  f"{key_data['errors']} errors {status}")
        
        print(f"\n🏆 ELITE KARMA TIER BREAKDOWN:")
        for tier in ['MEGA', 'SUPER', 'MAJOR']:
            count = tier_counts.get(tier, 0)
            if count > 0:
                percentage = (count / len(self.influencers)) * 100
                tier_karma = sum(i['total_karma'] for i in self.influencers if i['tier'] == tier)
                tier_avg = tier_karma / count
                tier_reach = sum(i['estimated_reach'] for i in self.influencers if i['tier'] == tier)
                print(f"  {tier:<6}: {count:>4,} accounts ({percentage:.1f}%) - Avg: {tier_avg:,.0f} karma - Reach: {tier_reach:,}")
        
        # Show premium account stats
        verified_count = sum(1 for i in self.influencers if i.get('is_verified'))
        premium_count = sum(1 for i in self.influencers if i.get('has_premium'))
        verified_email_count = sum(1 for i in self.influencers if i.get('has_verified_email'))
        
        print(f"\n🌟 ACCOUNT QUALITY METRICS:")
        print(f"  Reddit Employees: {verified_count}")
        print(f"  Premium Accounts: {premium_count} ({(premium_count/len(self.influencers))*100:.1f}%)")
        print(f"  Verified Email: {verified_email_count} ({(verified_email_count/len(self.influencers))*100:.1f}%)")
        
        # Top performers with profile links
        top_influencers = sorted(self.influencers, key=lambda x: x['total_karma'], reverse=True)[:25]
        
        print(f"\n🏆 TOP 25 ELITE INFLUENCERS:")
        print(f"{'Rank':<4} {'Username':<20} {'Karma':<12} {'Tier':<6} {'Per Day':<10} {'Reach':<12}")
        print(f"{'-'*4} {'-'*20} {'-'*12} {'-'*6} {'-'*10} {'-'*12}")
        for i, influencer in enumerate(top_influencers, 1):
            karma_per_day = influencer.get('karma_per_day', 0)
            reach = influencer.get('estimated_reach', 0)
            print(f"{i:2d}. u/{influencer['username']:<19} {influencer['total_karma']:>10,} {influencer['tier']:<6} {karma_per_day:>8.1f} {reach:>10,}")
        
        # Credit efficiency analysis
        if final_stats['total_credits_used'] > 0:
            efficiency = len(self.influencers) / final_stats['total_credits_used']
            print(f"\n💰 CREDIT EFFICIENCY ANALYSIS:")
            print(f"  Accounts per Credit: {efficiency:.3f}")
            print(f"  Cost per Elite Account: {1/efficiency:.1f} credits")
            print(f"  Estimated Free Plan Potential: {int(efficiency * (final_stats['total_keys'] * 1000)):,} accounts")
        
        print(f"\n📋 GITHUB ACTIONS OPTIMIZATION:")
        print(f"  • Multi-key rotation prevents single key exhaustion")
        print(f"  • Smart credit management maximizes free plan usage")
        print(f"  • Progress saving allows resumption after timeouts")
        print(f"  • CSV includes all account data with profile URLs")

def main() -> None:
    """Main execution optimized for GitHub Actions with multi-key support."""
    print("🏆 MULTI-KEY ELITE REDDIT SCRAPER - GITHUB ACTIONS OPTIMIZED")
    print("="*75)
    
    # Environment detection
    target_count = int(os.getenv('TARGET_COUNT', '2500'))
    
    try:
        scraper = EliteRedditScraperMultiKey(target_count=target_count)
    except ValueError as e:
        print(f"❌ Configuration Error: {e}")
        print("\n🔧 SETUP INSTRUCTIONS FOR GITHUB ACTIONS:")
        print("Add these secrets to your GitHub repository:")
        print("  • SCRAPERAPI_KEY - Your primary ScraperAPI key")
        print("  • SCRAPERAPI_KEY_2 - Additional key (optional)")
        print("  • SCRAPERAPI_KEY_3 - Additional key (optional)")
        print("Or set SCRAPERAPI_KEYS with comma-separated keys")
        return
    
    initial_stats = scraper.multi_key_config.get_stats_summary()
    print(f"🔑 Loaded {initial_stats['total_keys']} ScraperAPI keys")
    print(f"📊 Estimated scraping capacity: {initial_stats['total_keys'] * 1000} credits")
    print(f"🎯 Target: {target_count} elite influencers (50K+ karma)")
    print(f"🚀 GitHub Actions optimized with smart credit management")
    print("="*75)
    
    # Resume from previous progress if available
    resumed = scraper._load_progress()
    if resumed:
        print(f"🔄 Resumed from previous session: {len(scraper.influencers)} elite accounts loaded")
        print(f"⏩ Fast-forward: Skipping {len(scraper.scraped_users)} already processed users")
    
    try:
        start_time = time.time()
        scraper.scrape_all_categories_elite()
        
        # Final save and summary
        scraper._save_to_csv()
        scraper._save_progress()
        scraper.print_elite_summary()
        
        end_time = time.time()
        runtime_minutes = (end_time - start_time) / 60
        
        final_stats = scraper.multi_key_config.get_stats_summary()
        
        print(f"\n✅ MULTI-KEY ELITE SCRAPING COMPLETE!")
        print(f"📁 Results saved to: {scraper.csv_file}")
        print(f"📋 Log file: elite_scraper.log")
        print(f"🎯 Quality Achieved: Only 50K+ karma accounts")
        print(f"🔑 Keys Used: {final_stats['active_keys']}/{final_stats['total_keys']} active")
        print(f"💰 Credits Consumed: {final_stats['total_credits_used']}")
        print(f"⚡ Efficiency: {len(scraper.influencers)/max(final_stats['total_credits_used'], 1):.3f} accounts/credit")
        print(f"🔗 Profile URLs: Included for all accounts")
        print(f"⏱️  Total Runtime: {runtime_minutes:.1f} minutes")
        print(f"📈 Speed: {len(scraper.influencers)/max(runtime_minutes, 1):.1f} accounts/minute")
        print(f"🎯 GitHub Actions Compatible: ✅")
        
    except KeyboardInterrupt:
        print(f"\n⏹️  Elite scraping interrupted by user")
        scraper._save_to_csv('interrupted_' + scraper.csv_file)
        scraper._save_progress()
        scraper.print_elite_summary()
        print(f"💾 Progress saved - you can resume later!")
    
    except Exception as e:
        scraper.logger.error(f"❌ Critical error: {e}")
        if scraper.influencers:
            scraper._save_to_csv('error_' + scraper.csv_file)
            scraper._save_progress()
            scraper.print_elite_summary()
            print(f"💾 Data saved despite error - check error_{scraper.csv_file}")
        
        # Show final key stats even on error
        final_stats = scraper.multi_key_config.get_stats_summary()
        print(f"\n🔑 FINAL KEY STATISTICS:")
        print(f"Credits Used: {final_stats['total_credits_used']}")
        for key_name, key_data in final_stats['key_details'].items():
            print(f"  {key_name}: {key_data['credits']} credits, {'BLOCKED' if key_data['blocked'] else 'ACTIVE'}")

if __name__ == "__main__":
    main()
