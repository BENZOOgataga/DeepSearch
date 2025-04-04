# utils/cache_utils.py
import sys

def calculate_cache_sizes(member_cache, message_cache, user_cache, keyword_match_cache):
    """Calculate approximate memory usage of caches"""
    member_size = sum(sys.getsizeof(v) for v in member_cache.values()) / 1024
    message_size = sum(sys.getsizeof(v) for v in message_cache.values()) / 1024
    user_size = sum(sys.getsizeof(v) for v in user_cache.values()) / 1024
    keyword_size = sys.getsizeof(keyword_match_cache) / 1024
    total_cache_size = member_size + message_size + user_size + keyword_size

    return {
        "member_size": member_size,
        "message_size": message_size,
        "user_size": user_size,
        "keyword_size": keyword_size,
        "total_size": total_cache_size
    }

def get_cache_stats(member_cache, message_cache, user_cache, keyword_match_cache):
    """Get statistics about cached data"""
    cache_sizes = calculate_cache_sizes(member_cache, message_cache, user_cache, keyword_match_cache)

    return {
        "member_count": sum(len(members) for members in member_cache.values()),
        "member_guilds": len(member_cache),
        "message_entries": len(message_cache),
        "message_count": sum(len(messages) for messages in message_cache.values()),
        "user_count": len(user_cache),
        "keyword_matches": len(keyword_match_cache),
        "sizes": cache_sizes
    }