
#!/usr/bin/env python3
"""
ELITE Reddit Influencer Scraper with Multi-Key ScraperAPI Support - OPTIMIZED
Supports multiple ScraperAPI keys with intelligent rotation and credit management.
Designed for GitHub Actions with free plan credit optimization.
COMPLETE VERSION - All original functionality preserved and enhanced.
Updated with new karma tiers: LEGENDARY (5M+), RISING (25K+), MICRO (10K+)
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
        self.key_stats = {key: {"requests": 0, "errors": 0, "last_error_time": 0, "blocked": False, "credits_used": 0} 
                         for key in self.api_keys}
        self.current_key_index = 0
        self.max_requests_per_key = 4500  # Conservative for free plan (1000 credits)
        self.max_errors_per_key = 5  # Block key after consecutive errors
        self.error_reset_time = 3600  # Reset error count after 1 hour
        
    def _load_api_keys(self) -> List[str]:
        """Load multiple API keys from environment variables."""
        keys = []
        
        # Primary key
        primary_key = os.getenv("SCRAPE_KEY") or os.getenv("SCRAPERAPI_KEY")
        if primary_key:
            keys.append(primary_key)
        
        # Additional keys (SCRAPERAPI_KEY_2, SCRAPERAPI_KEY_3, etc.)
        key_index = 2
        while True:
            additional_key = os.getenv(f"SCRAPERAPI_KEY_{key_index}")
            if additional_key:
                keys.append(additional_key)
                key_index += 1
            else:
                break
        
        # Fallback keys from comma-separated string
        keys_string = os.getenv("SCRAPERAPI_KEYS")
        if keys_string:
            fallback_keys = [key.strip() for key in keys_string.split(",") if key.strip()]
            keys.extend(fallback_keys)
        
        # Built-in fallback key (your original)
        if not keys:
            keys.append("001dfb055d3443ea6a8ba1e0d2ac3562")
        
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
            if stats["errors"] > 0 and time.time() - stats["last_error_time"] > self.error_reset_time:
                stats["errors"] = 0
                stats["blocked"] = False
            
            # Check if key is usable
            if (not stats["blocked"] and 
                stats["requests"] < self.max_requests_per_key and 
                stats["errors"] < self.max_errors_per_key):
                return current_key
            
            # Move to next key
            self.current_key_index = (self.current_key_index + 1) % len(self.api_keys)
            attempts += 1
        
        # All keys exhausted or blocked, return least used key
        available_keys = [(key, stats) for key, stats in self.key_stats.items() 
                         if stats["requests"] < self.max_requests_per_key]
        
        if available_keys:
            best_key = min(available_keys, key=lambda x: x[1]["requests"])[0]
            self.current_key_index = self.api_keys.index(best_key)
            return best_key
        
        return None
    
    def record_request(self, api_key: str, success: bool = True, error_type: Optional[str] = None):
        """Record API request stats for intelligent rotation."""
        if api_key in self.key_stats:
            stats = self.key_stats[api_key]
            stats["requests"] += 1
            stats["credits_used"] += 1  # Assume 1 credit per request
            
            if not success:
                stats["errors"] += 1
                stats["last_error_time"] = time.time()
                
                # Block key if too many errors or specific error types
                if (stats["errors"] >= self.max_errors_per_key or 
                    error_type in ["403", "401", "quota_exceeded"]):
                    stats["blocked"] = True
            else:
                # Reset error count on successful request
                if stats["errors"] > 0:
                    stats["errors"] = max(0, stats["errors"] - 1)
    
    def get_stats_summary(self) -> Dict[str, Any]:
        """Get summary of all key statistics."""
        total_requests = sum(stats["requests"] for stats in self.key_stats.values())
        total_credits = sum(stats["credits_used"] for stats in self.key_stats.values())
        active_keys = sum(1 for stats in self.key_stats.values() if not stats["blocked"])
        
        return {
            "total_keys": len(self.api_keys),
            "active_keys": active_keys,
            "total_requests": total_requests,
            "total_credits_used": total_credits,
            "key_details": {
                f"Key_{i+1}": {
                    "requests": stats["requests"],
                    "credits": stats["credits_used"],
                    "errors": stats["errors"],
                    "blocked": stats["blocked"],
                    "last_8_chars": key[-8:] if len(key) >= 8 else key
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
            self.logger.error("ERROR: No ScraperAPI keys found! Add SCRAPERAPI_KEY or SCRAPERAPI_KEY_2, etc.")
            raise ValueError("No API keys available")
        else:
            stats = self.multi_key_config.get_stats_summary()
            self.logger.info(f"Multi-Key ScraperAPI enabled: {stats['total_keys']} keys loaded")
            for i, key in enumerate(self.multi_key_config.api_keys):
                self.logger.info(f"  Key {i+1}: ...{key[-8:]} (Ready)")
        
        # Progress tracking
        self.progress_file = "elite_scraper_progress.json"
        self.csv_file = "reddit_elite_influencers.csv"
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
        "crypto": [
            "ethfinance", "bitcoinmarkets", "CryptoMars", "pancakeswap",
            "uniswap", "aave_official", "yearn_finance", "compound_finance",
            "maticnetwork", "avalanche", "fantom", "terra_money", "cosmosnetwork",
            "cryptocurrency", "CryptoCurrency", "defi", "blockchain", "NFT", "dogecoin", "ethereum", "bitcoin"
        ],
        
        "tech": [
            "cscareerquestions", "ExperiencedDevs", "ITCareerQuestions", "sysadmin",
            "networking", "netsec", "hacking", "homelab", "selfhosted", "opensource",
            "github", "golang", "rust", "reactjs", "node", "angular", "vue",
            "programming", "Python", "javascript", "webdev", "androiddev", "iosdev", "gamedev", "linux", "cybersecurity"
        ],
        
        "investing": [
            "ecommerce", "dropship", "FBA", "amazonfba", "shopify", "sideproject",
            "indiehackers", "startupideas", "venturecapital", "sales",
            "Fire", "realestateinvesting", "landlord", "mortgages", "churning",
            "quantfinance", "algotrading", "daytrading", "forex", "futures",
            "stocks", "investing", "personalfinance", "wallstreetbets", "economy", "business"
        ],
        
        "gaming": [
            "pcgaming", "buildapc", "pcmasterrace", "nvidia", "amd", "intel",
            "PS5", "xbox", "nintendo", "NintendoSwitch", "Steam", "Genshin_Impact",
            "apexlegends", "FortNiteBR", "leagueoflegends", "Overwatch", "minecraft",
            "gaming", "gamernews", "esports", "WoW", "DestinyTheGame", "zelda", "pokemon"
        ],
        
        "entertainment": [
            "television", "netflix", "marvel", "DC_Cinematic", "StarWars",
            "gameofthrones", "HouseOfTheDragon", "stranger_things", "TheOffice",
            "rickandmorty", "anime", "manga", "OnePiece", "pokemon",
            "movies", "Music", "books", "comics", "youtubehaiku", "standupcomedy"
        ],
        
        "sports": [
            "nfl", "nba", "soccer", "baseball", "hockey", "MMA", "formula1",
            "fantasyfootball", "DynastyFF", "cycling", "bodybuilding", "powerlifting",
            "sports", "football", "basketball", "ufc", "WWE", "golf", "tennis"
        ],
        
        "lifestyle": [
            "relationship_advice", "AmItheAsshole", "legaladvice",
            "BuyItForLife", "cookingforbeginners", "DIY", "HomeImprovement",
            "gardening", "houseplants", "travel", "fitness", "nutrition", "meditation", "minimalism", "photography"
        ],
        
        "education": [
            "college", "ApplyingToCollege", "GradSchool", "GetStudying", "medicalschool",
            "lawschool", "MBA", "consulting", "accounting", "engineering",
            "learnprogramming", "AskHistorians", "science", "philosophy", "linguistics"
        ],
        
        "emerging_tech": [
            "ChatGPTCoding", "GPT3", "NoCode", "LowCode", "automation", "robotics", 
            "IoT", "edge_computing", "metaverse", "VRchat", "oculus", "virtualreality",
            "augmentedreality", "decentraland", "sandbox", "axieinfinity", "GameFi",
            "artificialintelligence", "machinelearning", "datascience", "futuretech"
        ]
}

        
        # Updated minimum karma threshold for new MICRO tier
        self.minimum_karma = 10000  # Lowered for MICRO tier (10K+)
        self.tier_priorities = ["LEGENDARY", "MEGA", "SUPER", "MAJOR", "RISING", "MICRO"]
        
    def _setup_logging(self) -> None:
        """Setup comprehensive logging."""
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s - %(levelname)s - %(message)s",
            handlers=[
                logging.FileHandler("elite_scraper.log"),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)
    
    def _create_sessions(self) -> List[requests.Session]:
        """Create multiple sessions with realistic browser headers."""
        user_agents = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2.1 Safari/605.1.15",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:123.0) Gecko/20100101 Firefox/123.0",
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Edge/122.0.0.0 Safari/537.36"
        ]
        
        sessions = []
        for ua in user_agents:
            session = requests.Session()
            
            session.headers.update({
                "User-Agent": ua,
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
                "Accept-Language": "en-US,en;q=0.9",
                "Accept-Encoding": "gzip, deflate, br",
                "DNT": "1",
                "Connection": "keep-alive",
                "Upgrade-Insecure-Requests": "1",
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
                self.logger.error("ERROR: All API keys exhausted or blocked!")
                # Wait and retry with least used key
                time.sleep(60)
                stats = self.multi_key_config.get_stats_summary()
                self.logger.info("Attempting with least used key after cooldown...")
                api_key = min(self.multi_key_config.api_keys, 
                             key=lambda k: self.multi_key_config.key_stats[k]["requests"])
            
            # Build ScraperAPI request
            gateway_params = {
                "api_key": api_key,
                "url": target_url,
                "render": "false",
                "country_code": "US",
                "premium": "false"  # Free plan optimization
            }
            
            session = self._get_session()
            kwargs_copy = kwargs.copy()
            kwargs_copy.pop("params", None)
            kwargs_copy["timeout"] = kwargs_copy.get("timeout", 25)
            
            try:
                response = session.get("http://api.scraperapi.com", params=gateway_params, **kwargs_copy)
                
                if response.status_code == 200:
                    # Success - record and return
                    self.multi_key_config.record_request(api_key, success=True)
                    return response
                elif response.status_code == 401:
                    # Invalid API key
                    self.multi_key_config.record_request(api_key, success=False, error_type="401")
                    self.logger.warning(f"Invalid API key ...{api_key[-8:]}, rotating to next key")
                elif response.status_code == 403:
                    # Quota exceeded or blocked
                    self.multi_key_config.record_request(api_key, success=False, error_type="403")
                    self.logger.warning(f"API key ...{api_key[-8:]} quota exceeded, rotating to next key")
                elif response.status_code == 429:
                    # Rate limited
                    self.multi_key_config.record_request(api_key, success=False, error_type="429")
                    self.logger.warning(f"API key ...{api_key[-8:]} rate limited, rotating to next key")
                    time.sleep(random.uniform(5, 15)) # Longer delay for rate limits
                else:
                    # Other errors
                    self.multi_key_config.record_request(api_key, success=False, error_type=str(response.status_code))
                    self.logger.error(f"ScraperAPI request failed with status {response.status_code}: {response.text}")
                
            except requests.exceptions.RequestException as e:
                self.multi_key_config.record_request(api_key, success=False, error_type="network_error")
                self.logger.error(f"Network error with ScraperAPI for key ...{api_key[-8:]}: {e}")
            
            attempt += 1
            self.current_session_idx = (self.current_session_idx + 1) % len(self.sessions) # Rotate session on error
            time.sleep(random.uniform(1, 3)) # Small delay before trying next key
            
        self.logger.error("All API keys failed after multiple attempts.")
        return requests.Response() # Return an empty response to indicate failure

    def _get_session(self) -> requests.Session:
        """Get the current requests session."""
        session = self.sessions[self.current_session_idx]
        self.current_session_idx = (self.current_session_idx + 1) % len(self.sessions)
        return session

    def _handle_rate_limit(self, response: requests.Response) -> bool:
        """Handle rate limiting responses."""
        if response.status_code == 429:
            self.logger.warning("Rate limit hit. Waiting...")
            time.sleep(random.uniform(10, 20))  # Wait longer for rate limits
            return True
        return False

    def _adaptive_delay(self) -> None:
        """Implement adaptive delay based on request count and success rate."""
        self.request_count += 1
        elapsed_time = time.time() - self.session_start
        
        # Adjust delay based on overall request rate
        if self.request_count % 50 == 0: # Re-evaluate every 50 requests
            avg_time_per_request = elapsed_time / self.request_count
            target_avg_time = 0.5 # Aim for 2 requests per second per key
            
            if avg_time_per_request < target_avg_time:
                self.current_delay = min(self.current_delay * 1.1, 5.0) # Increase delay
            else:
                self.current_delay = max(self.current_delay * 0.9, self.base_delay) # Decrease delay
        
        time.sleep(self.current_delay + random.uniform(0, 0.5)) # Add some jitter

    def _save_progress(self) -> None:
        """Save current progress to a JSON file."""
        progress_data = {
            "influencers": self.influencers,
            "scraped_users": list(self.scraped_users),
            "failed_users": list(self.failed_users),
            "multi_key_config_stats": self.multi_key_config.key_stats
        }
        try:
            with open(self.progress_file, "w") as f:
                json.dump(progress_data, f, indent=4)
            self.logger.info(f"Progress saved to {self.progress_file}")
        except Exception as e:
            self.logger.error(f"Failed to save progress: {e}")

    def _load_progress(self) -> None:
        """Load progress from a JSON file."""
        if os.path.exists(self.progress_file):
            try:
                with open(self.progress_file, "r") as f:
                    progress_data = json.load(f)
                self.influencers = progress_data.get("influencers", [])
                self.scraped_users = set(progress_data.get("scraped_users", []))
                self.failed_users = set(progress_data.get("failed_users", []))
                self.multi_key_config.key_stats = progress_data.get("multi_key_config_stats", self.multi_key_config.key_stats)
                self.logger.info(f"Progress loaded from {self.progress_file}. {len(self.influencers)} influencers found.")
            except Exception as e:
                self.logger.error(f"Failed to load progress: {e}")
        else:
            self.logger.info("No existing progress file found.")

    def _save_to_csv(self, filename: Optional[str] = None) -> None:
        """Save elite influencers to CSV with Reddit profile URLs and Niche."""
        output_filename = filename if filename else self.csv_file
        
        if not self.influencers:
            self.logger.info("No influencers to save to CSV.")
            return

        # Ensure all expected headers are present, including 'niche'
        fieldnames = list(self.influencers[0].keys())
        if "niche" not in fieldnames:
            fieldnames.append("niche")
        
        try:
            with open(output_filename, "w", newline="", encoding="utf-8") as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(self.influencers)
            self.logger.info(f"Successfully saved {len(self.influencers)} influencers to {output_filename}")
        except Exception as e:
            self.logger.error(f"Failed to save CSV: {e}")

    def get_elite_posts_only(self, subreddit: str, limit: int = 100, max_retries: int = 2) -> List[Dict[str, Any]]:
        """Get only TOP and HOT posts using multi-key ScraperAPI."""
        all_posts = []
        
        # Streamlined sort configs for speed
        sort_configs = [
            ("hot", None),      # Currently trending
            ("top", "week"),    # Best of the week
            ("top", "month"),   # Best of the month
        ]
        
        for sort_type, time_filter in sort_configs:
            url = f"https://www.reddit.com/r/{subreddit}/{sort_type}.json"
            params: Dict[str, Union[str, int]] = {"limit": limit}
            
            if sort_type == "top" and time_filter is not None:
                params["t"] = time_filter
            
            for attempt in range(max_retries):
                try:
                    response = self._multi_key_scraperapi_request(url, params=params)
                    
                    if self._handle_rate_limit(response):
                        continue
                        
                    response.raise_for_status()
                    data = response.json()
                    posts = data.get("data", {}).get("children", [])
                    
                    all_posts.extend(posts)
                    self._adaptive_delay()
                    break
                    
                except Exception as e:
                    self.logger.warning(f"Attempt {attempt + 1} failed for r/{subreddit} ({sort_type}): {e}")
                    if attempt < max_retries - 1:
                        time.sleep(random.uniform(2, 6))
                    else:
                        self.logger.error(f"Failed to get {sort_type} posts from r/{subreddit}")
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
                user_data = data.get("data", {})
                
                if user_data:
                    self.scraped_users.add(username)
                    self._adaptive_delay()
                    return user_data
                else:
                    self.failed_users.add(username)
                    return {}
                
            except Exception as e:
                self.logger.warning(f"Attempt {attempt + 1} failed for user {username}: {e}")
                if attempt < max_retries - 1:
                    time.sleep(random.uniform(2, 6))
                else:
                    self.logger.error(f"Failed to get user {username} after {max_retries} attempts")
                    self.failed_users.add(username)
                    return {}
        
        return {}
    
    def _determine_niche(self, subreddit_name: str) -> str:
        """Determines the niche based on the subreddit name."""
        for niche, subreddits in self.subreddit_categories.items():
            if subreddit_name in subreddits:
                return niche
        return "general" # Default niche if not found

    def analyze_elite_user(self, user_data: Dict[str, Any], subreddit_name: str) -> Dict[str, Any]:
        """ELITE USER ANALYSIS with new karma tiers and niche detection."""
        if not user_data:
            return {}
        
        username = user_data.get("name", "")
        link_karma = user_data.get("link_karma", 0)
        comment_karma = user_data.get("comment_karma", 0)
        total_karma = link_karma + comment_karma
        
        # UPDATED ELITE FILTERING for new MICRO tier
        if total_karma < self.minimum_karma:  # Now 10,000 for MICRO tier
            return {}
        
        if user_data.get("is_suspended", False):
            return {}
        
        if username in ["[deleted]", "", None]:
            return {}
        
        account_age_days = (datetime.now().timestamp() - user_data.get("created_utc", 0)) / 86400
        
        if account_age_days < 90:
            return {}
        
        karma_per_day = total_karma / max(account_age_days, 1)
        
        # UPDATED ELITE TIER CLASSIFICATION with new tiers
        if total_karma >= 5000000:  # 5M+ karma
            tier = "LEGENDARY"
            reach_multiplier = 5.0
        elif total_karma >= 1000000:  # 1M+ karma
            tier = "MEGA"
            reach_multiplier = 4.0
        elif total_karma >= 250000:  # 250K+ karma
            tier = "SUPER" 
            reach_multiplier = 3.0
        elif total_karma >= 50000:  # 50K+ karma
            tier = "MAJOR"
            reach_multiplier = 2.5
        elif total_karma >= 25000:  # 25K+ karma
            tier = "RISING"
            reach_multiplier = 2.0
        elif total_karma >= 10000:  # 10K+ karma
            tier = "MICRO"
            reach_multiplier = 1.5
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
            "username": username,
            "reddit_profile_url": reddit_profile_url,
            "total_karma": total_karma,
            "link_karma": link_karma,
            "comment_karma": comment_karma,
            "account_age_days": int(account_age_days),
            "karma_per_day": round(karma_per_day, 2),
            "tier": tier,
            "estimated_reach": estimated_reach,
            "is_verified": user_data.get("is_employee", False),
            "has_premium": user_data.get("is_gold", False),
            "has_verified_email": user_data.get("has_verified_email", False),
            "scraped_at": datetime.now().isoformat(),
            "niche": self._determine_niche(subreddit_name) # Add niche here
        }
        
        return influencer
    
    def scrape_subreddit_elite(self, subreddit: str, initial_limit: int = 10, max_limit: int = 100) -> int:
        """Optimized elite subreddit scraping with adaptive logic and multi-key support."""
        self.logger.info(f"ELITE scraping r/{subreddit} (initial check: {initial_limit} posts)")
        
        found_users = 0
        posts_to_check = []
        
        # Phase 1: Initial quick check
        posts = self.get_elite_posts_only(subreddit, initial_limit)
        posts.sort(key=lambda x: x.get("data", {}).get("score", 0) if x.get("data") else 0, reverse=True)
        
        elite_found_in_initial_check = 0
        for post_data in posts:
            post = post_data.get("data", {})
            username = post.get("author", "")
            post_score = post.get("score", 0)
            
            if post_score < 500: # Still keep a minimum score for efficiency
                continue
            
            if (username and username != "[deleted]" and 
                username not in self.scraped_users and 
                username not in self.failed_users):
                
                user_data = self.get_user_with_retry(username)
                influencer = self.analyze_elite_user(user_data, subreddit) # Pass subreddit name
                
                if influencer:
                    self.influencers.append(influencer)
                    found_users += 1
                    elite_found_in_initial_check += 1
                    self.logger.info(f"Added u/{username} ({influencer['tier']}, {influencer['total_karma']:,} karma, Niche: {influencer['niche']})")
                    
                    if len(self.influencers) % self.backup_interval == 0:
                        self._save_progress()
                        self.logger.info(f"Progress saved: {len(self.influencers)}/{self.target_count}")
        
        self.logger.info(f"Initial check found {elite_found_in_initial_check} elite users in r/{subreddit}")

        # Phase 2: Adaptive scraping based on initial findings
        if elite_found_in_initial_check >= initial_limit * 0.5: # If more than 50% of initial checks yielded elite users
            self.logger.info(f"Subreddit r/{subreddit} is rich, scraping more (up to {max_limit} posts).")
            remaining_posts_limit = max_limit - initial_limit
            if remaining_posts_limit > 0:
                additional_posts = self.get_elite_posts_only(subreddit, remaining_posts_limit)
                additional_posts.sort(key=lambda x: x.get("data", {}).get("score", 0) if x.get("data") else 0, reverse=True)
                posts_to_check.extend(additional_posts)
        elif elite_found_in_initial_check > 0: # If some elite users were found, but not a lot
            self.logger.info(f"Subreddit r/{subreddit} has some elite users, scraping a bit more (up to {initial_limit * 2} posts).")
            additional_posts_limit = initial_limit # Scrape another 'initial_limit' posts
            additional_posts = self.get_elite_posts_only(subreddit, additional_posts_limit)
            additional_posts.sort(key=lambda x: x.get("data", {}).get("score", 0) if x.get("data") else 0, reverse=True)
            posts_to_check.extend(additional_posts)
        else:
            self.logger.info(f"Subreddit r/{subreddit} seems to have no elite users, moving on.")
            return found_users # No elite users found, stop here to save credits

        # Continue scraping from the additional posts
        for post_data in posts_to_check:
            if len(self.influencers) >= self.target_count:
                break
            
            # Check if we're approaching credit limits
            stats = self.multi_key_config.get_stats_summary()
            if stats["total_credits_used"] > (stats["total_keys"] * 4500):  # 90% of credits used
                self.logger.warning("Approaching credit limits, saving progress...")
                self._save_progress()
                break # Stop scraping this subreddit if credits are low
            
            post = post_data.get("data", {})
            username = post.get("author", "")
            post_score = post.get("score", 0)
            
            if post_score < 500:
                continue
            
            if (username and username != "[deleted]" and 
                username not in self.scraped_users and 
                username not in self.failed_users):
                
                user_data = self.get_user_with_retry(username)
                influencer = self.analyze_elite_user(user_data, subreddit) # Pass subreddit name
                
                if influencer:
                    self.influencers.append(influencer)
                    found_users += 1
                    
                    self.logger.info(f"Added u/{username} ({influencer['tier']}, {influencer['total_karma']:,} karma, Niche: {influencer['niche']})")
                    
                    if len(self.influencers) % self.backup_interval == 0:
                        self._save_progress()
                        self.logger.info(f"Progress saved: {len(self.influencers)}/{self.target_count}")
        
        self.logger.info(f"Found {found_users} elite users from r/{subreddit}")
        return found_users
    
    def scrape_all_categories_elite(self) -> None:
        """Scrape all categories with multi-key credit management."""
        self.logger.info(f"MULTI-KEY ELITE SCRAPING - Targeting {self.target_count} top-tier influencers")
        self.logger.info("Minimum Karma: 10,000 (MICRO tier and above)")
        self.logger.info("Tiers: LEGENDARY (5M+), MEGA (1M+), SUPER (250K+), MAJOR (50K+), RISING (25K+), MICRO (10K+)")
        
        initial_stats = self.multi_key_config.get_stats_summary()
        self.logger.info(f"Starting with {initial_stats['total_keys']} API keys, "
                        f"{initial_stats['total_credits_used']} credits already used")
        
        category_stats: Dict[str, int] = {category: 0 for category in self.subreddit_categories.keys()}
        
        # Prioritize categories by potential for high-karma users
        priority_order = [
            "crypto",
            "tech",
            "investing",
            "gaming",
            "entertainment",
            "sports",
            "lifestyle",
            "education",
            "emerging_tech"
        ]
        
        for category_name in priority_order:
            if len(self.influencers) >= self.target_count:
                break
                
            # Check credit usage before starting new category
            current_stats = self.multi_key_config.get_stats_summary()
            if current_stats["active_keys"] == 0:
                self.logger.error("No active API keys remaining!")
                break
            elif current_stats["total_credits_used"] > (current_stats["total_keys"] * 4750): # 95% of credits used
                self.logger.warning("95% of credits used, stopping to preserve resources")
                break
                
            subreddits = self.subreddit_categories[category_name]
            self.logger.info(f"\nCATEGORY: {category_name.upper()} ({len(subreddits)} subreddits)")
            self.logger.info(f"Active keys: {current_stats['active_keys']}/{current_stats['total_keys']}, "
                           f"Credits used: {current_stats['total_credits_used']}")
            
            # Shuffle for variety but maintain quality focus
            random.shuffle(subreddits)
            
            for subreddit in subreddits:
                if len(self.influencers) >= self.target_count:
                    break
                
                # Double-check credit status
                live_stats = self.multi_key_config.get_stats_summary()
                if live_stats["active_keys"] == 0:
                    self.logger.warning("All keys exhausted during category processing")
                    break
                
                try:
                    # Adaptive scraping: initial_limit=10, max_limit=100
                    added_count = self.scrape_subreddit_elite(subreddit, initial_limit=10, max_limit=100)
                    category_stats[category_name] += added_count
                    
                    # Progress logging every 100 accounts
                    if len(self.influencers) % 100 == 0 and len(self.influencers) > 0:
                        progress = (len(self.influencers) / self.target_count) * 100
                        live_stats = self.multi_key_config.get_stats_summary()
                        self.logger.info(f"Progress: {len(self.influencers)}/{self.target_count} ({progress:.1f}%) "
                                       f"| Credits: {live_stats['total_credits_used']}")
                    
                except Exception as e:
                    self.logger.error(f"Error scraping r/{subreddit}: {e}")
                    continue
                
                # Show tier distribution every 250 accounts
                if len(self.influencers) % 250 == 0 and len(self.influencers) > 0:
                    self._show_elite_distribution()
        
        # Final category breakdown
        final_stats = self.multi_key_config.get_stats_summary()
        self.logger.info(f"\nFINAL CATEGORY BREAKDOWN:")
        for category, count in category_stats.items():
            percentage = (count / len(self.influencers)) * 100 if self.influencers else 0
            self.logger.info(f"  {category.upper():<30}: {count:>4,} ({percentage:.1f}%)")
        
        self.logger.info(f"\nFINAL MULTI-KEY STATISTICS:")
        for key_name, key_data in final_stats["key_details"].items():
            self.logger.info(f"  {key_name}: {key_data['requests']} requests, "
                           f"{key_data['credits']} credits, "
                           f"{'BLOCKED' if key_data['blocked'] else 'ACTIVE'}")
    
    def print_elite_summary(self) -> None:
        """Prints a summary of scraped elite influencers."""
        self.logger.info(f"\n--- ELITE INFLUENCER SUMMARY ---")
        self.logger.info(f"Total Elite Influencers Scraped: {len(self.influencers)}")
        
        tier_counts: Dict[str, int] = {}
        total_karma = 0
        niche_counts: Dict[str, int] = {}

        for influencer in self.influencers:
            tier = influencer["tier"]
            tier_counts[tier] = tier_counts.get(tier, 0) + 1
            total_karma += influencer["total_karma"]
            niche = influencer.get("niche", "unknown")
            niche_counts[niche] = niche_counts.get(niche, 0) + 1

        avg_karma = total_karma / len(self.influencers) if self.influencers else 0
        self.logger.info(f"Average Karma per Influencer: {avg_karma:,.0f}")

        self.logger.info(f"\nElite Tier Distribution:")
        for tier in self.tier_priorities:
            count = tier_counts.get(tier, 0)
            if count > 0:
                percentage = (count / len(self.influencers)) * 100
                self.logger.info(f"  {tier:<9}: {count:>4,} ({percentage:.1f}%)")

        self.logger.info(f"\nNiche Distribution:")
        for niche, count in niche_counts.items():
            percentage = (count / len(self.influencers)) * 100
            self.logger.info(f"  {niche:<15}: {count:>4,} ({percentage:.1f}%)")

        self.logger.info(f"----------------------------------")

    def _show_elite_distribution(self) -> None:
        """Show current elite tier and niche distribution."""
        tier_counts: Dict[str, int] = {}
        total_karma = 0
        niche_counts: Dict[str, int] = {}
        
        for influencer in self.influencers:
            tier = influencer["tier"]
            tier_counts[tier] = tier_counts.get(tier, 0) + 1
            total_karma += influencer["total_karma"]
            niche = influencer.get("niche", "unknown")
            niche_counts[niche] = niche_counts.get(niche, 0) + 1
        
        avg_karma = total_karma / len(self.influencers) if self.influencers else 0
        
        self.logger.info(f"ELITE TIER DISTRIBUTION (Avg: {avg_karma:,.0f} karma):")
        for tier in ["LEGENDARY", "MEGA", "SUPER", "MAJOR", "RISING", "MICRO"]:
            count = tier_counts.get(tier, 0)
            if count > 0:
                percentage = (count / len(self.influencers)) * 100
                tier_karma = sum(i["total_karma"] for i in self.influencers if i["tier"] == tier)
                tier_avg = tier_karma / count
                self.logger.info(f"    {tier:<9}: {count:>4,} ({percentage:.1f}%) - Avg: {tier_avg:,.0f} karma")

        self.logger.info(f"Niche Distribution:")
        for niche, count in niche_counts.items():
            percentage = (count / len(self.influencers)) * 100
            self.logger.info(f"    {niche:<15}: {count:>4,} ({percentage:.1f}%)")


def main():
    scraper = EliteRedditScraperMultiKey(target_count=2500)
    scraper._load_progress()
    
    try:
        start_time = time.time()
        scraper.scrape_all_categories_elite()
        scraper._save_to_csv()
        scraper._save_progress()
        scraper.print_elite_summary()
        
        end_time = time.time()
        runtime_minutes = (end_time - start_time) / 60
        
        final_stats = scraper.multi_key_config.get_stats_summary()
        
        print(f"\nMULTI-KEY ELITE SCRAPING COMPLETE!")
        print(f"Results saved to: {scraper.csv_file}")
        print(f"Log file: elite_scraper.log")
        print(f"Quality Achieved: Only 10K+ karma accounts")
        print(f"Keys Used: {final_stats['active_keys']}/{final_stats['total_keys']} active")
        print(f"Credits Consumed: {final_stats['total_credits_used']}")
        print(f"Efficiency: {len(scraper.influencers)/max(final_stats['total_credits_used'], 1):.3f} accounts/credit")
        print(f"Profile URLs: Included for all accounts")
        print(f"Total Runtime: {runtime_minutes:.1f} minutes")
        print(f"Speed: {len(scraper.influencers)/max(runtime_minutes, 1):.1f} accounts/minute")
        print(f"GitHub Actions Compatible: YES")
        
    except KeyboardInterrupt:
        print(f"\nElite scraping interrupted by user")
        scraper._save_to_csv("interrupted_" + scraper.csv_file)
        scraper._save_progress()
        scraper.print_elite_summary()
        print(f"Progress saved - you can resume later!")
    
    except Exception as e:
        scraper.logger.error(f"Critical error: {e}")
        if scraper.influencers:
            scraper._save_to_csv("error_" + scraper.csv_file)
            scraper._save_progress()
            scraper.print_elite_summary()
            print(f"Data saved despite error - check error_{scraper.csv_file}")
        
        # Show final key stats even on error
        final_stats = scraper.multi_key_config.get_stats_summary()
        print(f"\nFINAL KEY STATISTICS:")
        print(f"Credits Used: {final_stats['total_credits_used']}")
        for key_name, key_data in final_stats["key_details"].items():
            print(f"  {key_name}: {key_data['credits']} credits, {'BLOCKED' if key_data['blocked'] else 'ACTIVE'}")

if __name__ == "__main__":
    main()


