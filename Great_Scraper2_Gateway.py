#!/usr/bin/env python3
"""
ELITE Reddit Influencer Scraper with Gateway Support - OPTIMIZED
Integrates residential-IP scraping gateways to avoid datacenter IP bans.
Supports ScraperAPI, ScrapFly, and Bright Data MCP.
OPTIMIZED for speed while maintaining gateway protection.
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

class GatewayConfig:
    """Configuration for different scraping gateway providers."""
    
    PROVIDERS = {
        'scraperapi': {
            'base_url': 'http://api.scraperapi.com',
            'params_key': 'api_key',
            'extra_params': {
                'render': 'false',
                'country_code': 'US',
                'premium': 'false'
            }
        },
        'scrapfly': {
            'base_url': 'https://api.scrapfly.io/scrape',
            'params_key': 'key',
            'extra_params': {
                'render_js': 'false',
                'country': 'US',
                'asp': 'false'
            }
        },
        'brightdata': {
            'base_url': 'https://brd-customer-hl_XXXXXXXXX-zone-residential_proxy1.brd.superproxy.io:22225',
            'auth': True,  # Uses basic auth instead of API key
            'proxy_style': True
        }
    }

class EliteRedditScraperWithGateway:
    """Elite scraper with integrated gateway support to avoid datacenter IP bans."""
    
    def __init__(self, target_count: int = 2500, gateway_provider: str = 'scraperapi') -> None:
        self.target_count = target_count
        self.influencers: List[Dict[str, Any]] = []
        self.scraped_users: Set[str] = set()
        self.failed_users: Set[str] = set()
        
        # Gateway configuration with hardcoded ScraperAPI key
        self.gateway_provider = gateway_provider
        self.gateway_config = GatewayConfig.PROVIDERS.get(gateway_provider)
        # Use environment variable first, then fallback to hardcoded key
        self.gateway_key = os.getenv('SCRAPE_KEY') or os.getenv('GATEWAY_API_KEY') or '001dfb055d3443ea6a8ba1e0d2ac3562'
        self.gateway_enabled = bool(self.gateway_key and self.gateway_config)
        
        # Setup logging first
        self._setup_logging()
        
        if not self.gateway_enabled:
            self.logger.warning("⚠️ No gateway configured! Running in direct mode (may get banned on CI/CD)")
        else:
            self.logger.info(f"🛡️ Gateway enabled: {gateway_provider.upper()}")
            self.logger.info(f"🔑 Using API key: {self.gateway_key[:8]}...")
        
        # Progress tracking
        self.progress_file = 'elite_scraper_progress.json'
        self.csv_file = 'reddit_elite_influencers.csv'
        self.backup_interval = 50
        
        # OPTIMIZED rate limiting for speed while maintaining gateway protection
        self.request_count = 0
        self.session_start = time.time()
        self.max_requests_per_hour = 5000 if self.gateway_enabled else 3500  # Increased for gateway
        self.base_delay = 0.3 if self.gateway_enabled else 0.8  # Much faster with gateway
        self.current_delay = self.base_delay
        
        # Session management - optimized
        self.sessions = self._create_sessions()
        self.current_session_idx = 0
        
        # Enhanced subreddit list - same as version 2
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
            
            # Configure retries - optimized for speed
            try:
                from requests.adapters import HTTPAdapter
                from urllib3.util.retry import Retry
                
                retry_strategy = Retry(
                    total=3,  # Reduced retries for speed
                    backoff_factor=1.5,  # Faster backoff
                    status_forcelist=[403, 429, 500, 502, 503, 504],
                )
                adapter = HTTPAdapter(max_retries=retry_strategy)
                session.mount("http://", adapter)
                session.mount("https://", adapter)
            except ImportError:
                pass
            
            sessions.append(session)
            
        return sessions
    
    def _gateway_request(self, url: str, params: Optional[Dict] = None, **kwargs) -> requests.Response:
        """Optimized gateway request method."""
        if not self.gateway_enabled:
            # Direct request (original behavior)
            session = self._get_session()
            return session.get(url, params=params, timeout=20, **kwargs)
        
        # Gateway request - optimized for ScraperAPI
        session = self._get_session()
        
        if self.gateway_provider == 'scraperapi':
            return self._scraperapi_request_optimized(url, params, session, **kwargs)
        elif self.gateway_provider == 'scrapfly':
            return self._scrapfly_request(url, params, session, **kwargs)
        elif self.gateway_provider == 'brightdata':
            return self._brightdata_request(url, params, session, **kwargs)
        else:
            # Fallback to direct
            return session.get(url, params=params, timeout=20, **kwargs)
    
    def _scraperapi_request_optimized(self, url: str, params: Optional[Dict], session: requests.Session, **kwargs) -> requests.Response:
        """Optimized ScraperAPI request - faster processing."""
        # Build target URL with params
        if params:
            from urllib.parse import urlencode
            query_string = urlencode(params)
            target_url = f"{url}?{query_string}"
        else:
            target_url = url
        
        # Streamlined gateway params for speed
        gateway_params = {
            'api_key': self.gateway_key,
            'url': target_url,
            'render': 'false',  # Always false for speed
            'country_code': 'US'
        }
        
        # Remove params from kwargs since we're embedding them in the URL
        kwargs.pop('params', None)
        # Set optimized timeout
        kwargs['timeout'] = kwargs.get('timeout', 25)
        
        return session.get('http://api.scraperapi.com', params=gateway_params, **kwargs)
    
    def _scrapfly_request(self, url: str, params: Optional[Dict], session: requests.Session, **kwargs) -> requests.Response:
        """Make request through ScrapFly."""
        if params:
            from urllib.parse import urlencode
            query_string = urlencode(params)
            target_url = f"{url}?{query_string}"
        else:
            target_url = url
        
        scrapfly_params = {
            'key': self.gateway_key,
            'url': target_url,
            'render_js': 'false',
            'country': 'US',
            'asp': 'false'
        }
        
        kwargs.pop('params', None)
        kwargs['timeout'] = kwargs.get('timeout', 25)
        return session.get('https://api.scrapfly.io/scrape', params=scrapfly_params, **kwargs)
    
    def _brightdata_request(self, url: str, params: Optional[Dict], session: requests.Session, **kwargs) -> requests.Response:
        """Make request through Bright Data (proxy style)."""
        # Note: This requires proxy credentials in format username:password
        proxy_url = os.getenv('BRIGHTDATA_PROXY_URL')  # e.g., 'brd-customer-hl_XXXXX-zone-residential_proxy1:password@brd.superproxy.io:22225'
        
        if proxy_url:
            proxies = {
                'http': f'http://{proxy_url}',
                'https': f'http://{proxy_url}'
            }
            kwargs['proxies'] = proxies
        
        kwargs['timeout'] = kwargs.get('timeout', 25)
        return session.get(url, params=params, **kwargs)
    
    def _get_session(self) -> requests.Session:
        """Get current session and rotate if needed."""
        session = self.sessions[self.current_session_idx]
        
        # Rotate sessions more frequently for better distribution
        if self.request_count % 200 == 0 and self.request_count > 0:
            self.current_session_idx = (self.current_session_idx + 1) % len(self.sessions)
            self.logger.info(f"🔄 Rotated to session {self.current_session_idx}")
            
        return session
    
    def _adaptive_delay(self) -> None:
        """Optimized adaptive delay - much faster with gateway."""
        self.request_count += 1
        
        elapsed_hours = (time.time() - self.session_start) / 3600
        if elapsed_hours > 0:
            requests_per_hour = self.request_count / elapsed_hours
            
            if requests_per_hour > self.max_requests_per_hour * 0.8:
                self.current_delay = min(self.current_delay * 1.1, 2.0 if self.gateway_enabled else 4.0)
            elif requests_per_hour < self.max_requests_per_hour * 0.6:
                self.current_delay = max(self.current_delay * 0.95, self.base_delay)
        
        # Reduced jitter for speed
        jitter = random.uniform(0.8, 1.2) if self.gateway_enabled else random.uniform(0.5, 1.5)
        actual_delay = self.current_delay * jitter
        
        time.sleep(actual_delay)
    
    def _handle_rate_limit(self, response: requests.Response) -> bool:
        """Optimized rate limiting - faster recovery with gateway."""
        if response.status_code == 429:
            self.logger.warning("⚠️ Rate limited! Implementing exponential backoff")
            backoff_time = random.uniform(15, 45) if self.gateway_enabled else random.uniform(60, 180)
            self.logger.info(f"😴 Sleeping for {backoff_time:.1f} seconds")
            time.sleep(backoff_time)
            self.current_delay *= 1.2 if self.gateway_enabled else 1.5
            return True
        elif response.status_code in [502, 503, 504]:
            self.logger.warning(f"🔧 Server error {response.status_code}, retrying...")
            time.sleep(random.uniform(3, 8) if self.gateway_enabled else random.uniform(10, 30))
            return True
        elif response.status_code == 403 and not self.gateway_enabled:
            self.logger.error("🚫 403 Forbidden - datacenter IP likely banned! Consider using a gateway.")
            return True
        return False
    
    def _save_progress(self) -> None:
        """Save current progress to resume later."""
        progress_data = {
            'total_scraped': len(self.influencers),
            'scraped_users': list(self.scraped_users),
            'failed_users': list(self.failed_users),
            'timestamp': datetime.now().isoformat(),
            'request_count': self.request_count
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
        """Get only TOP and HOT posts through gateway - OPTIMIZED."""
        all_posts = []
        
        # Streamlined sort configs for speed
        sort_configs = [
            ('hot', None),      # Currently trending (fastest)  
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
                    # Use optimized gateway request method
                    response = self._gateway_request(url, params=params)
                    
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
        """Optimized user retrieval with faster retry logic."""
        if username in self.scraped_users or username in self.failed_users:
            return {}
        
        url = f"https://www.reddit.com/user/{username}/about.json"
        
        for attempt in range(max_retries):
            try:
                # Use optimized gateway request method
                response = self._gateway_request(url)
                
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
        """ELITE USER ANALYSIS - Only accept high-karma accounts with Reddit profile link."""
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
            'reddit_profile_url': reddit_profile_url,  # NEW: Reddit account link
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
        """Optimized elite subreddit scraping with gateway support."""
        self.logger.info(f"🎯 ELITE scraping r/{subreddit} (targeting {target_users} top users)")
        
        found_users = 0
        posts = self.get_elite_posts_only(subreddit, 100)
        
        # Sort posts by score to prioritize high-engagement content
        posts.sort(key=lambda x: x.get('data', {}).get('score', 0), reverse=True)
        
        for post_data in posts:
            if found_users >= target_users or len(self.influencers) >= self.target_count:
                break
            
            post = post_data.get('data', {})
            username = post.get('author', '')
            post_score = post.get('score', 0)
            
            # Lower threshold for faster results but still quality-focused
            if post_score < 500:  # Lowered from 1000 for more candidates
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
        """Scrape all categories focusing on elite accounts only - OPTIMIZED."""
        self.logger.info(f"🚀 ELITE SCRAPING MODE - Targeting {self.target_count} top-tier influencers")
        self.logger.info("🏆 Minimum Karma: 50,000 (MAJOR tier and above)")
        self.logger.info("🎯 Tiers: MEGA (1M+), SUPER (250K+), MAJOR (50K+)")
        
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
                
            subreddits = self.subreddit_categories[category_name]
            self.logger.info(f"\n🎯 CATEGORY: {category_name.upper()} ({len(subreddits)} subreddits)")
            
            # Shuffle for variety but maintain quality focus
            random.shuffle(subreddits)
            
            # Optimized targets per subreddit - balance speed vs quality
            users_per_subreddit = 25  # Reduced for faster processing
            
            for subreddit in subreddits:
                if len(self.influencers) >= self.target_count:
                    break
                
                try:
                    added_count = self.scrape_subreddit_elite(subreddit, users_per_subreddit)
                    category_stats[category_name] += added_count
                    
                    # Fast progress logging every 100 accounts
                    if len(self.influencers) % 100 == 0 and len(self.influencers) > 0:
                        progress = (len(self.influencers) / self.target_count) * 100
                        self.logger.info(f"📈 FAST Progress: {len(self.influencers)}/{self.target_count} ({progress:.1f}%)")
                    
                except Exception as e:
                    self.logger.error(f"❌ Error scraping r/{subreddit}: {e}")
                    continue
                
                # Show tier distribution every 250 accounts (less frequent for speed)
                if len(self.influencers) % 250 == 0 and len(self.influencers) > 0:
                    self._show_elite_distribution()
        
        # Final category breakdown
        self.logger.info(f"\n📊 FINAL CATEGORY BREAKDOWN:")
        for category, count in category_stats.items():
            percentage = (count / len(self.influencers)) * 100 if self.influencers else 0
            self.logger.info(f"  {category.upper():<30}: {count:>4,} accounts ({percentage:.1f}%)")
    
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
        """Print comprehensive elite summary."""
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
        
        print(f"\n🏆 ELITE SCRAPING RESULTS")
        print(f"{'='*60}")
        print(f"✅ Total Elite Influencers: {len(self.influencers):,}")
        print(f"🎯 Target Achievement: {(len(self.influencers)/self.target_count)*100:.1f}%")
        print(f"📊 Combined Karma: {total_karma:,}")
        print(f"📈 Average Karma: {avg_karma:,.0f}")
        print(f"📈 Estimated Total Reach: {total_reach:,}")
        print(f"⏱️  Total Requests: {self.request_count:,}")
        print(f"🕐 Runtime: {(time.time() - self.session_start)/3600:.1f} hours")
        print(f"🛡️  Gateway: {'ENABLED ✅' if self.gateway_enabled else 'DISABLED ⚠️'}")
        print(f"🔥 Quality Focus: 50K+ karma minimum")
        print(f"🔗 Profile URLs: Included in CSV export")
        
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
        print(f"{'Rank':<4} {'Username':<20} {'Karma':<12} {'Tier':<6} {'Per Day':<10} {'Reach':<12} {'Profile URL'}")
        print(f"{'-'*4} {'-'*20} {'-'*12} {'-'*6} {'-'*10} {'-'*12} {'-'*50}")
        for i, influencer in enumerate(top_influencers, 1):
            karma_per_day = influencer.get('karma_per_day', 0)
            reach = influencer.get('estimated_reach', 0)
            profile_url = influencer.get('reddit_profile_url', 'N/A')
            print(f"{i:2d}. u/{influencer['username']:<19} {influencer['total_karma']:>10,} {influencer['tier']:<6} {karma_per_day:>8.1f} {reach:>10,} {profile_url}")
        
        # Karma distribution analysis
        mega_accounts = [i for i in self.influencers if i['tier'] == 'MEGA']
        if mega_accounts:
            mega_avg = sum(i['total_karma'] for i in mega_accounts) / len(mega_accounts)
            print(f"\n💎 MEGA TIER ANALYSIS ({len(mega_accounts)} accounts):")
            print(f"  Average Karma: {mega_avg:,.0f}")
            print(f"  Combined Reach: {sum(i['estimated_reach'] for i in mega_accounts):,}")
        
        print(f"\n📋 CSV Export includes:")
        print(f"  • Username and Reddit profile URL")
        print(f"  • Complete karma breakdown")
        print(f"  • Account quality metrics")
        print(f"  • Estimated reach calculations")

def main() -> None:
    """Main execution with optimized gateway configuration."""
    print("🏆 ELITE REDDIT INFLUENCER SCRAPER WITH GATEWAY SUPPORT - OPTIMIZED")
    print("="*70)
    
    # Detect gateway configuration
    gateway_provider = os.getenv('GATEWAY_PROVIDER', 'scraperapi').lower()
    gateway_key = os.getenv('SCRAPE_KEY') or os.getenv('GATEWAY_API_KEY') or '001dfb055d3443ea6a8ba1e0d2ac3562'
    
    if gateway_key and gateway_key != '001dfb055d3443ea6a8ba1e0d2ac3562':
        print(f"🛡️  Gateway: {gateway_provider.upper()} (Environment key) ✅")
    elif gateway_key:
        print(f"🛡️  Gateway: {gateway_provider.upper()} (Built-in key) ✅")
    else:
        print("⚠️  No gateway configured - running in direct mode")
    
    print("🚀 OPTIMIZATION: Faster delays, reduced retries, streamlined processing")
    print("🔗 NEW FEATURE: Reddit profile URLs included in export")
    print("Target: Top-Tier Accounts Only (50K+ karma)")
    print("Focus: MEGA (1M+), SUPER (250K+), MAJOR (50K+)")
    print("="*70)
    
    scraper = EliteRedditScraperWithGateway(target_count=2500, gateway_provider=gateway_provider)
    
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
        
        print(f"\n✅ ELITE SCRAPING COMPLETE!")
        print(f"📁 Results saved to: {scraper.csv_file}")
        print(f"📋 Log file: elite_scraper.log")
        print(f"🎯 Quality Achieved: Only 50K+ karma accounts")
        print(f"🛡️  Gateway Protection: {'ACTIVE ✅' if scraper.gateway_enabled else 'INACTIVE ⚠️'}")
        print(f"⚡ Speed Optimization: {'ENABLED' if scraper.gateway_enabled else 'LIMITED'}")
        print(f"🔗 Profile URLs: Included for all accounts")
        print(f"⏱️  Total Runtime: {runtime_minutes:.1f} minutes")
        print(f"📈 Speed: {len(scraper.influencers)/max(runtime_minutes, 1):.1f} accounts/minute")
        
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

if __name__ == "__main__":
    main()