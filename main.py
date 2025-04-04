import asyncio
import json
import logging
import os
import platform
import re
import sys
import time
from datetime import datetime
from datetime import timedelta

import aiofiles
import discord
import psutil
from cachetools import TTLCache
from discord.ext import commands, tasks
from dotenv import load_dotenv
from utils.search_utils import process_search_channels, update_search_status, update_search_stats
from utils.command_utils import setup_command_execution, handle_cancel_request, apply_cooldown
from utils.cache_utils import get_cache_stats


# --- Custom RotatingFileHandler for async ---
class AsyncLogHandler(logging.Handler):
    def __init__(self, filename, mode='a', encoding='utf-8'):
        super().__init__()
        self.filename = filename
        self.mode = mode
        self.encoding = encoding
        self.queue = asyncio.Queue()
        self.task = None
        self.loop = None

    async def start(self):
        self.loop = asyncio.get_running_loop()
        self.task = asyncio.create_task(self._worker())

    async def stop(self):
        if self.task:
            self.queue.put_nowait(None)
            await self.task
            self.task = None

    async def _worker(self):
        while True:
            record = await self.queue.get()
            if record is None:
                break

            try:
                async with aiofiles.open(self.filename, self.mode, encoding=self.encoding) as f:
                    await f.write(self.format(record) + '\n')
            except Exception:
                self.handleError(record)

    def emit(self, record):
        if self.loop is None or self.task is None:
            # Fall back to synchronous if not started
            try:
                with open(self.filename, self.mode, encoding=self.encoding) as f:
                    f.write(self.format(record) + '\n')
            except Exception:
                self.handleError(record)
            return

        # Put record in queue for async processing
        try:
            self.queue.put_nowait(record)
        except Exception:
            self.handleError(record)

# --- Load env and config ---
load_dotenv()
TOKEN = os.getenv("BOT_TOKEN")
CONFIG_FILE = "config.json"

with open(CONFIG_FILE, "r", encoding="utf-8") as f:
    CONFIG = json.load(f)

# --- Setup logs ---
LOGS_DIR = "logs"
os.makedirs(LOGS_DIR, exist_ok=True)
msg_log_path = os.path.join(LOGS_DIR, "message_logs.log")
user_log_path = os.path.join(LOGS_DIR, "user_logs.log")


def setup_logging():
    # Create date-based directory structure
    current_datetime = datetime.now()
    date_hour_dir = os.path.join(LOGS_DIR, current_datetime.strftime('%Y-%m-%d_%H'))
    os.makedirs(date_hour_dir, exist_ok=True)

    # Define log paths with date-based directory
    msg_log_path = os.path.join(date_hour_dir, "message_logs.log")
    user_log_path = os.path.join(date_hour_dir, "user_logs.log")

    # Configure async log handlers
    msg_handler = AsyncLogHandler(msg_log_path, mode='a', encoding='utf-8')
    user_handler = AsyncLogHandler(user_log_path, mode='a', encoding='utf-8')

    formatter = logging.Formatter('%(asctime)s - %(message)s')
    msg_handler.setFormatter(formatter)
    user_handler.setFormatter(formatter)

    msg_logger = logging.getLogger('message_log')
    user_logger = logging.getLogger('user_log')

    msg_logger.setLevel(logging.INFO)
    user_logger.setLevel(logging.INFO)

    msg_logger.propagate = False
    user_logger.propagate = False

    # Clear any existing handlers
    if msg_logger.handlers:
        for handler in msg_logger.handlers:
            msg_logger.removeHandler(handler)
    if user_logger.handlers:
        for handler in user_logger.handlers:
            user_logger.removeHandler(handler)

    msg_logger.addHandler(msg_handler)
    user_logger.addHandler(user_handler)

    return msg_logger, user_logger, msg_log_path, user_log_path


# --- Init bot ---
intents = discord.Intents.all()
intents.members = True
intents.message_content = True
bot = commands.Bot(command_prefix="!", intents=intents, help_command=None)

msg_logger, user_logger, msg_log_path, user_log_path = setup_logging()

# --- Caches ---
member_cache = TTLCache(maxsize=500, ttl=3600)  # Cache for 1 hour, store up to 500 guilds
message_cache = TTLCache(maxsize=1000, ttl=300)  # Cache for 5 minutes, store up to 1000 channel histories
user_cache = TTLCache(maxsize=2000, ttl=3600)  # Cache for 1 hour, store up to 2000 users
keyword_match_cache = TTLCache(maxsize=10000, ttl=3600)  # Cache for 1 hour


# --- Utils ---
def save_config():
    try:
        with open(CONFIG_FILE, "w", encoding="utf-8") as f:
            json.dump(CONFIG, f, indent=2)
    except Exception as e:
        print(f"Error saving config: {e}")


def log_to_file(path, content):
    try:
        with open(path, "a", encoding="utf-8") as f:
            f.write(content)
    except Exception as e:
        print(f"Error writing to log {path}: {e}")


def is_admin(ctx):
    return ctx.author.guild_permissions.administrator


def format_time_interval(minutes):
    """Format time interval in appropriate units"""
    if minutes < 1:
        # Less than a minute, show seconds
        seconds = int(minutes * 60)
        return f"{seconds} seconds"
    elif minutes < 60:
        # Less than an hour, show minutes
        return f"{minutes} minutes"
    elif minutes < 1440:  # 24 hours
        # Less than a day, show hours and minutes
        hours = int(minutes // 60)
        mins = int(minutes % 60)
        if mins == 0:
            return f"{hours} hours"
        else:
            return f"{hours} hours, {mins} minutes"
    elif minutes < 10080:  # 7 days
        # Less than a week, show days and hours
        days = int(minutes // 1440)
        hours = int((minutes % 1440) // 60)
        if hours == 0:
            return f"{days} days"
        else:
            return f"{days} days, {hours} hours"
    else:
        # More than a week, show weeks and days
        weeks = int(minutes // 10080)
        days = int((minutes % 10080) // 1440)
        if days == 0:
            return f"{weeks} weeks"
        else:
            return f"{weeks} weeks, {days} days"


def update_scheduled_tasks():
    """Update scheduled tasks based on current config"""
    if CONFIG.get("auto_scan_enabled", False):
        interval = CONFIG.get("auto_scan_interval_minutes", 60)

        # Convert to seconds for more precision
        seconds = int(interval * 60)

        # Need to restart the task to change interval
        if auto_scan.is_running():
            auto_scan.cancel()

        # Update interval and restart
        auto_scan.change_interval(seconds=seconds)
        auto_scan.start()

        print(f"🔄 Auto-scan enabled (every {format_time_interval(interval)})")
    else:
        if auto_scan.is_running():
            auto_scan.cancel()
            print("🛑 Auto-scan disabled")


def get_cached_members(guild_id):
    """Get or create cached member list for a guild"""
    if guild_id not in member_cache:
        guild = bot.get_guild(guild_id)
        if guild:
            member_cache[guild_id] = list(guild.members)
    return member_cache.get(guild_id, [])


async def get_cached_messages(channel_id, limit=100, force_refresh=False):
    """Get or create cached message history for a channel"""
    cache_key = f"{channel_id}_{limit}"
    if force_refresh or cache_key not in message_cache:
        channel = bot.get_channel(channel_id)
        if channel:
            messages = []
            async for msg in channel.history(limit=limit):
                messages.append(msg)
            message_cache[cache_key] = messages
    return message_cache.get(cache_key, [])


async def get_cached_user(user_id):
    """Get or create cached user information"""
    if user_id not in user_cache:
        try:
            user = await bot.fetch_user(int(user_id))
            user_cache[user_id] = user
        except Exception:
            return None
    return user_cache.get(user_id)


# Create a set of lowercase keywords for faster matching
KEYWORD_SET = {k.lower() for k in CONFIG["search_keywords"]}


def keyword_match(text):
    """Check if text contains any keywords with caching"""
    # Use a short hash of the text as a cache key
    cache_key = hash(text) % 10000000

    if cache_key in keyword_match_cache:
        return keyword_match_cache[cache_key]

    # Convert text to lowercase once
    text_lower = text.lower()

    # Check for any keyword in the set
    result = any(k in text_lower for k in KEYWORD_SET)

    # Cache the result
    keyword_match_cache[cache_key] = result
    return result


def parse_query_limit(limit_str):
    """Parse query limit with support for k/m suffixes (e.g., 5k = 5000)"""
    limit_str = limit_str.lower()
    try:
        if limit_str.endswith('k'):
            return int(float(limit_str[:-1]) * 1000)
        elif limit_str.endswith('m'):
            return int(float(limit_str[:-1]) * 1000000)
        else:
            return int(limit_str)
    except ValueError:
        return None


# Unified argument parsing function
def parse_command_args(args):
    """Parse command arguments and separate flags from positional arguments"""
    processed_args = []
    flags = {}
    i = 0

    while i < len(args):
        arg = args[i].lower()

        # Handle flags that take a value
        if i + 1 < len(args) and arg in ("--q", "--query"):
            limit = parse_query_limit(args[i + 1])
            if limit is not None:
                flags["query_limit"] = limit
                i += 1  # Skip the value
            else:
                flags["error"] = "Invalid query limit. Must be a number (e.g., 100, 5k, 1m)."
        elif i + 1 < len(args) and arg in ("--in", "--channel"):
            flags["include_channels"] = [c.strip() for c in args[i + 1].split(",")]
            i += 1  # Skip the value
        elif i + 1 < len(args) and arg in ("--exclude", "--not"):
            flags["exclude_channels"] = [c.strip() for c in args[i + 1].split(",")]
            i += 1  # Skip the value
        # Handle boolean flags
        elif arg in ("--all", "-a"):
            flags["deep_search"] = True
        elif arg in ("--users", "-u"):
            flags["scan_users"] = True
        elif arg in ("--messages", "-m"):
            flags["scan_messages"] = True
        else:
            processed_args.append(args[i])

        i += 1

    return processed_args, flags


# --- Events ---
@bot.event
async def on_ready():
    for handler in msg_logger.handlers + user_logger.handlers:
        if isinstance(handler, AsyncLogHandler):
            await handler.start()
    print(f"✅ Logged in as {bot.user}")
    print("🔍 Keywords:", ', '.join(CONFIG["search_keywords"]))
    print(f"📁 Logs at {os.path.dirname(msg_log_path)}")

    update_scheduled_tasks()
    print()

    # Track message matches for initial scan
    initial_message_matches = 0
    initial_member_matches = 0

    for guild in bot.guilds:
        print(f"🔍 Scanning guild: {guild.name} ({guild.id})")
        # Chunk only if necessary
        if not guild.chunked:
            await guild.chunk(cache=True)

        # Scan members
        for member in get_cached_members(guild.id):
            name_fields = f"{member.name} {member.display_name}"
            if keyword_match(name_fields):
                initial_member_matches += 1
                entry = f"[AUTO] {member} ({member.id}) in {guild.name}"
                user_logger.info(entry)
                if CONFIG["print_user_matches"]:
                    print(entry)

        # Initial scan of 5000 messages
        print(f"🔍 Scanning 5k messages in {guild.name}...")
        message_scan_count = 0
        channels = [c for c in guild.text_channels if c.permissions_for(guild.me).read_messages]

        if channels:
            # Calculate messages per channel to reach approximately 5000 total
            messages_per_channel = max(1, 5000 // len(channels))

            for channel in channels:
                try:
                    async for msg in channel.history(limit=messages_per_channel):
                        message_scan_count += 1
                        if keyword_match(msg.content):
                            initial_message_matches += 1
                            entry = f"[INIT] {msg.author} in #{channel.name} ({guild.name}) > {msg.content}"
                            msg_logger.info(entry)
                            if CONFIG["print_message_matches"]:
                                print(entry)
                except discord.Forbidden:
                    continue
                except Exception as e:
                    print(f"Error scanning {channel.name}: {e}")

        print(f"✅ Scanned {len(guild.members)} members and {message_scan_count} messages in {guild.name}")

    print(f"✅ Initial scan complete! Found {initial_member_matches} matching members and {initial_message_matches} matching messages.")


@bot.event
async def on_message(msg):
    if msg.author.bot or not msg.guild:
        return
    if keyword_match(msg.content):
        entry = f"[AUTO] {msg.author} in #{msg.channel} ({msg.guild.name}) > {msg.content}"
        msg_logger.info(entry)
        if CONFIG["print_message_matches"]:
            print(entry)
    await bot.process_commands(msg)


# --- Commands ---
@bot.command(name="setkeywords")
async def set_keywords(ctx, *, words):
    if not is_admin(ctx):
        return await ctx.send("❌ You must be a server admin to use this.")
    new_words = [w.strip() for w in words.split(",") if w.strip()]
    CONFIG["search_keywords"] = new_words
    save_config()
    await ctx.send(f"✅ Keywords updated: {', '.join(new_words)}")


@bot.command(name="toggleprints")
async def toggle_prints(ctx, category: str):
    if not is_admin(ctx):
        return await ctx.send("❌ You must be a server admin to use this.")
    if category not in ["user", "message"]:
        return await ctx.send("⚠️ Use `user` or `message`.")
    key = f"print_{category}_matches"
    CONFIG[key] = not CONFIG[key]
    save_config()
    await ctx.send(f"✅ {category.capitalize()} print set to {CONFIG[key]}")


@bot.command(name="showkeywords")
async def show_keywords(ctx):
    if not is_admin(ctx):
        return await ctx.send("❌ You must be a server admin to use this.")
    await ctx.send("🔍 Current keywords:\n" + ", ".join(CONFIG["search_keywords"]))


@bot.command(name="scan")
async def scan_members(ctx, *args):
    if not is_admin(ctx):
        return await ctx.send("❌ You must be a server admin to use this.")

    global search_cancelled

    # Handle cancel request
    if len(args) == 1 and args[0].lower() == "cancel":
        if hasattr(scan_members, "is_running") and scan_members.is_running:
            search_cancelled = True
            return await ctx.send("⚠️ Scan cancelled.")
        else:
            return await ctx.send("⚠️ No scan is currently running.")

    if not hasattr(scan_members, "is_running"):
        scan_members.is_running = False

    if scan_members.is_running:
        return await ctx.send("⚠️ A scan is already running. Please wait for it to complete or use `!scan cancel` to stop it.")

    search_cancelled = False

    # Use the unified argument parsing function
    processed_args, flags = parse_command_args(args)

    # Extract flags with default values
    deep_search = flags.get("deep_search", False)
    query_limit = flags.get("query_limit", 500)  # Default limit
    custom_query = "query_limit" in flags
    include_channels = flags.get("include_channels", [])
    exclude_channels = flags.get("exclude_channels", [])
    scan_users = flags.get("scan_users", False)
    scan_messages = flags.get("scan_messages", False)

    # If no specific scan type is selected, inform the user
    if not scan_users and not scan_messages:
        if "deep_search" in flags or "query_limit" in flags:
            # User specified search options but not what to scan
            scan_messages = True  # Default to messages if options were provided
        else:
            return await ctx.send("⚠️ Usage: `!scan [--users/-u] [--messages/-m] [--all/-a] [--q limit]`")

    # If deep_search is enabled, consider both users and messages
    if deep_search and not (scan_users or scan_messages):
        scan_users = True
        scan_messages = True

    # Create a description of what's being scanned
    scan_targets = []
    if scan_users:
        scan_targets.append("members")
    if scan_messages:
        scan_targets.append("messages")
    scan_description = " and ".join(scan_targets)

    # Fix: Capitalize first letter when not deep searching
    scanning_text = "Scanning" if not deep_search else "Deep scanning"
    status_msg = await ctx.send(f"🔍 {scanning_text} {scan_description}...")

    scan_members.is_running = True

    try:
        user_count = 0
        message_count = 0
        start_time = datetime.now()
        last_update_time = start_time

        # Scan members if requested
        if scan_users:
            # Ensure guild is chunked for complete member list
            if not ctx.guild.chunked:
                await ctx.guild.chunk(cache=True)

            total_members = len(ctx.guild.members)
            members_scanned = 0

            for member in ctx.guild.members:
                members_scanned += 1

                # Update status periodically
                current_time = datetime.now()
                if (current_time - last_update_time).total_seconds() > 5:
                    progress = members_scanned / total_members * 100
                    time_elapsed = (current_time - start_time).total_seconds()
                    await status_msg.edit(content=f"🔍 {scanning_text} members... ({members_scanned}/{total_members}, {progress:.1f}%, {time_elapsed:.1f}s)")
                    last_update_time = current_time

                # Check for cancellation
                if search_cancelled:
                    await status_msg.edit(content=f"⚠️ Scan cancelled after checking {members_scanned} members.")
                    return

                name_fields = f"{member.name} {member.display_name}"
                if keyword_match(name_fields):
                    user_count += 1
                    entry = f"[SCAN] {member.name} ({member.id}) in {ctx.guild.name}"
                    user_logger.info(entry)
                    if CONFIG["print_user_matches"]:
                        print(entry)

        # Scan messages if requested
        if scan_messages:
            # Prepare search channels
            search_channels = []
            if include_channels:
                for ch_name in include_channels:
                    ch_name = ch_name.strip('#')
                    channel = discord.utils.get(ctx.guild.text_channels, name=ch_name)
                    if channel:
                        search_channels.append(channel)
            else:
                if exclude_channels:
                    exclude_ch_names = [ch.strip('#') for ch in exclude_channels]
                    search_channels = [ch for ch in ctx.guild.text_channels
                                       if ch.name not in exclude_ch_names]
                else:
                    search_channels = ctx.guild.text_channels

            total_channels = len(search_channels)
            channels_scanned = 0
            total_messages_scanned = 0

            for channel in search_channels:
                channels_scanned += 1

                # Check for cancellation
                if search_cancelled:
                    await status_msg.edit(content=f"⚠️ Scan cancelled after scanning {channels_scanned}/{total_channels} channels.")
                    return

                # Update status periodically
                current_time = datetime.now()
                if (current_time - last_update_time).total_seconds() > 5:
                    progress = channels_scanned / total_channels * 100
                    time_elapsed = (current_time - start_time).total_seconds()
                    await status_msg.edit(content=f"🔍 {scanning_text} messages... ({channels_scanned}/{total_channels} channels, {total_messages_scanned} msgs, {progress:.1f}%, {time_elapsed:.1f}s)")
                    last_update_time = current_time

                try:
                    # Use different limits based on deep search setting
                    limit = query_limit if deep_search or custom_query else 100
                    async for msg in channel.history(limit=limit):
                        total_messages_scanned += 1

                        if keyword_match(msg.content):
                            message_count += 1
                            entry = f"[SCAN] {msg.author} in #{channel.name} ({ctx.guild.name}) > {msg.content}"
                            msg_logger.info(entry)
                            if CONFIG["print_message_matches"]:
                                print(entry)
                except discord.Forbidden:
                    continue

        # Calculate scan time
        scan_time = (datetime.now() - start_time).total_seconds()

        # Format the result message
        result_parts = []
        if scan_users:
            result_parts.append(f"{user_count} matching members")
        if scan_messages:
            result_parts.append(f"{message_count} matching messages (from {total_messages_scanned} messages)")

        result_text = " and ".join(result_parts)
        await status_msg.edit(content=f"✅ Scan complete! Found {result_text} in {scan_time:.1f}s.")

    except Exception as e:
        await ctx.send(f"⚠️ Error during scan: {e}")
    finally:
        scan_members.is_running = False
        search_cancelled = False

# Define search cooldowns dictionary
search_cooldowns = {}

# Global search cancellation flag
search_cancelled = False


@bot.command(name="search")
async def search_messages(ctx, *args):
    global search_cancelled, search_cooldowns

    # Handle cancel request
    cancel_result = handle_cancel_request(search_messages, args)
    if cancel_result is not None:
        if cancel_result:
            search_cancelled = True
            return await ctx.send("⚠️ Search cancelled.")
        else:
            return await ctx.send("⚠️ No search is currently running.")

    if not is_admin(ctx):
        return await ctx.send("❌ You must be a server admin to use this.")

    # Setup command execution
    if not setup_command_execution(search_messages):
        return await ctx.send("⚠️ A search is already running. Please wait for it to complete or use `!search cancel` to stop it.")

    search_cancelled = False

    # Use the unified argument parsing function
    processed_args, flags = parse_command_args(args)

    # Extract flags with default values
    deep_search = flags.get("deep_search", False)
    query_limit = flags.get("query_limit", 500)  # Default limit
    custom_query = "query_limit" in flags
    include_channels = flags.get("include_channels", [])
    exclude_channels = flags.get("exclude_channels", [])

    # Check if there was an error in parsing arguments
    if "error" in flags:
        search_messages.is_running = False
        return await ctx.send(f"⚠️ {flags['error']}")

    # Check for required user and keyword arguments
    if len(processed_args) < 2:
        search_messages.is_running = False
        return await ctx.send("⚠️ Usage: `!search @user keyword [--a/--all] [--q limit] [--in #channel1,#channel2] [--exclude #channel3]`")

    # Extract user from the first processed argument
    try:
        user_arg = processed_args[0]
        if user_arg.startswith("<@") and user_arg.endswith(">"):
            user_id = user_arg[2:-1].strip()
            if user_id.startswith('!'):
                user_id = user_id[1:]
        else:
            user_id = user_arg

        user_id = int(user_id)
        user = await get_cached_user(user_id)
        if not user:
            search_messages.is_running = False
            return await ctx.send("⚠️ User not found. Please make sure you've provided a valid user ID or @mention.")

        # Extract keyword from remaining processed arguments
        keyword = " ".join(processed_args[1:])
    except Exception:
        search_messages.is_running = False
        return await ctx.send("⚠️ Invalid user format. Use @mention or user ID.")

    # Apply cooldown check for deep searches
    cooldown_ok, remaining = apply_cooldown(search_cooldowns, ctx, deep_search, custom_query)
    if not cooldown_ok:
        search_messages.is_running = False
        return await ctx.send(f"⚠️ Please wait {remaining:.1f} minutes before performing another deep search in this server.")

    # Prepare search channels
    search_channels = await process_search_channels(ctx, include_channels, exclude_channels)
    if not search_channels:
        search_messages.is_running = False
        return

    # Status message based on search type
    search_msg_prefix = ""
    if include_channels:
        channel_names = ", ".join([f"#{ch.name}" for ch in search_channels])
        search_msg_prefix = f" in channels: {channel_names}"
    elif exclude_channels:
        channel_names = ", ".join(exclude_channels)
        search_msg_prefix = f" (excluding channels: {channel_names})"

    # Fix: Capitalize first letter when not deep searching
    searching_text = "Searching" if not deep_search else "Deep searching"
    status_msg = await ctx.send(
        f"🔍 {searching_text} for messages from {user.name} containing '{keyword}'{search_msg_prefix}..."
    )

    try:
        total_channels = len(search_channels)
        found_messages = []
        total_searched = 0
        start_time = datetime.now()
        last_update_time = start_time
        channels_searched = 0

        for channel in search_channels:
            channels_searched += 1

            # Check for cancellation
            if search_cancelled:
                await status_msg.edit(content=f"⚠️ Search cancelled after checking {channels_searched}/{total_channels} channels.")
                return

            try:
                # Use different limits based on deep search setting
                limit = query_limit if deep_search or custom_query else 100

                messages_checked = 0
                async for msg in channel.history(limit=limit):
                    messages_checked += 1
                    total_searched += 1

                    # Update status message periodically
                    last_update_time = await update_search_status(
                        status_msg,
                        channels_searched,
                        total_channels,
                        total_searched,
                        len(found_messages),
                        start_time,
                        last_update_time,
                        search_cancelled
                    )

                    # Check for cancellation
                    if search_cancelled:
                        break

                    # Check if message is from target user and contains keyword
                    if msg.author.id == user.id and keyword.lower() in msg.content.lower():
                        found_messages.append(msg)

            except discord.Forbidden:
                continue
            except Exception as e:
                await ctx.send(f"⚠️ Error searching channel {channel.name}: {e}")
                continue

        # Calculate search time
        search_time = (datetime.now() - start_time).total_seconds()

        if not found_messages:
            await status_msg.edit(content=f"✅ Search complete! No messages found from {user.name} containing '{keyword}' (searched {total_searched:,} messages in {search_time:.1f}s)")
        else:
            # Format results
            result_text = f"✅ Found {len(found_messages)} messages from {user.name} containing '{keyword}' (searched {total_searched:,} messages in {search_time:.1f}s)"

            # List first few results
            result_text += "\nLatest messages:"
            count = 0
            for msg in sorted(found_messages, key=lambda m: m.created_at, reverse=True):
                if count < 5:  # Show at most 5 messages
                    channel_name = msg.channel.name
                    date = msg.created_at.strftime('%Y-%m-%d %H:%M:%S')
                    result_text += f"\n- {date} #{channel_name}: {msg.content[:100]}{'...' if len(msg.content) > 100 else ''}"
                    count += 1
                else:
                    break

            result_text += f"\n\nUse `!export {user.id} {keyword}` to export all messages."
            await status_msg.edit(content=result_text[:2000])  # Discord message limit

        # Update search statistics
        update_search_stats(search_stats, ctx, total_searched, found_messages, search_time)
        save_search_stats()

    finally:
        search_messages.is_running = False
        search_cancelled = False


# Handle cooldown error
@search_messages.error
async def search_error(ctx, error):
    if isinstance(error, commands.CommandOnCooldown):
        remaining = int(error.retry_after)
        minutes = remaining // 60
        seconds = remaining % 60
        await ctx.send(f"⚠️ Command on cooldown! Try again in {minutes}m {seconds}s.")


STATS_FILE = "search_stats.json"


def load_search_stats():
    """Load search statistics from file if it exists"""
    try:
        if os.path.exists(STATS_FILE):
            with open(STATS_FILE, 'r', encoding='utf-8') as f:
                loaded_stats = json.load(f)
                return loaded_stats
        else:
            # Return default stats structure if file doesn't exist
            return {
                "total_searches": 0,
                "total_messages_searched": 0,
                "total_matches_found": 0,
                "cancelled_searches": 0,
                "deep_searches": 0,
                "search_time_total": 0,
                "searches_by_guild": {},
                "searches_by_user": {},
                "last_search": None,
                "largest_search": {"messages": 0, "time": 0, "keyword": "", "guild": ""}
            }
    except Exception as e:
        print(f"Error loading search stats: {e}")
        # Return default stats structure on error
        return {
            "total_searches": 0,
            "total_messages_searched": 0,
            "total_matches_found": 0,
            "cancelled_searches": 0,
            "deep_searches": 0,
            "search_time_total": 0,
            "searches_by_guild": {},
            "searches_by_user": {},
            "last_search": None,
            "largest_search": {"messages": 0, "time": 0, "keyword": "", "guild": ""}
        }


# Global dictionaries for tracking search stats
search_stats = load_search_stats()


def save_search_stats():
    """Save current search statistics to file"""
    try:
        with open(STATS_FILE, 'w', encoding='utf-8') as f:
            json.dump(search_stats, f, indent=2)
    except Exception as e:
        print(f"Error saving search stats: {e}")


@bot.command(name="searchstats")
async def search_stats_command(ctx):
    """Show statistics about searches performed"""
    if not is_admin(ctx):
        return await ctx.send("❌ You must be a server admin to use this.")

    global search_stats

    # No stats available
    if search_stats["total_searches"] == 0:
        return await ctx.send("ℹ️ No search statistics available yet.")

    # Calculate average time and messages
    avg_time = search_stats["search_time_total"] / search_stats["total_searches"]
    avg_messages = search_stats["total_messages_searched"] / search_stats["total_searches"]

    # Create embed with stats
    embed = discord.Embed(
        title="🔍 Search Statistics",
        color=0x3498db,
        description="Statistics about searches performed with this bot"
    )

    # General stats
    embed.add_field(
        name="General Stats",
        value=f"• Total searches: **{search_stats['total_searches']}**\n"
              f"• Messages searched: **{search_stats['total_messages_searched']:,}**\n"
              f"• Matches found: **{search_stats['total_matches_found']}**\n"
              f"• Deep searches: **{search_stats['deep_searches']}**\n"
              f"• Cancelled searches: **{search_stats['cancelled_searches']}**",
        inline=False
    )

    # Performance stats
    embed.add_field(
        name="Performance",
        value=f"• Average messages per search: **{avg_messages:,.1f}**\n"
              f"• Average search time: **{avg_time:.2f}s**\n"
              f"• Total search time: **{search_stats['search_time_total']:.2f}s**",
        inline=False
    )

    # Last search info
    if search_stats["last_search"]:
        last = search_stats["last_search"]
        embed.add_field(
            name="Last Search",
            value=f"• User: **{last['user']}**\n"
                  f"• Keyword: **{last['keyword']}**\n"
                  f"• Messages: **{last['messages']:,}**\n"
                  f"• Time: **{last['time']:.2f}s**\n"
                  f"• Matches: **{last['matches']}**\n"
                  f"• Guild: **{last['guild']}**",
            inline=False
        )

    # Largest search info
    if search_stats["largest_search"]["messages"] > 0:
        largest = search_stats["largest_search"]
        embed.add_field(
            name="Largest Search",
            value=f"• Messages: **{largest['messages']:,}**\n"
                  f"• Time: **{largest['time']:.2f}s**\n"
                  f"• Keyword: **{largest['keyword']}**\n"
                  f"• Guild: **{largest['guild']}**",
            inline=False
        )

    # Most frequent searchers
    if search_stats["searches_by_user"]:
        top_users = sorted(search_stats["searches_by_user"].items(), key=lambda x: x[1], reverse=True)[:5]
        users_text = "\n".join([f"• **{user}**: {count} searches" for user, count in top_users])
        embed.add_field(name="Top Searchers", value=users_text, inline=False)

    # Most searched servers
    if search_stats["searches_by_guild"]:
        top_guilds = sorted(search_stats["searches_by_guild"].items(), key=lambda x: x[1], reverse=True)[:5]
        guilds_text = "\n".join([f"• **{guild}**: {count} searches" for guild, count in top_guilds])
        embed.add_field(name="Most Searched Servers", value=guilds_text, inline=False)

    # Footer - update to show stats are persistent
    embed.set_footer(text="Stats persist across bot restarts")

    await ctx.send(embed=embed)

    # Save stats to ensure they're synced with file
    save_search_stats()


@bot.command(name="context")
async def get_context(ctx, message_id: int = None, lines: int = 5):
    """Get context around a specific message"""
    if not is_admin(ctx):
        return await ctx.send("❌ You must be a server admin to use this.")

    # Check for required message ID
    if message_id is None:
        return await ctx.send("⚠️ Usage: `!context message_id [lines=5]`\nYou must provide a message ID to get context.")

    # Validate lines parameter
    if lines < 1:
        lines = 1
    elif lines > 15:  # Limit to reasonable number
        lines = 15
        await ctx.send("⚠️ Maximum context limited to 15 messages.")

    # Status message
    status_msg = await ctx.send(f"🔍 Searching for message {message_id} and retrieving {lines} messages of context...")

    # Find the message across all channels
    target_message = None
    target_channel = None

    for channel in ctx.guild.text_channels:
        try:
            # First check cached messages to avoid unnecessary API calls
            messages = await get_cached_messages(channel.id, limit=100)
            for msg in messages:
                if msg.id == message_id:
                    target_message = msg
                    target_channel = channel
                    break

            if target_message:
                break

            # If not found in cache, try to fetch directly
            target_message = await channel.fetch_message(message_id)
            if target_message:
                target_channel = channel
                break
        except (discord.NotFound, discord.Forbidden, discord.HTTPException):
            # Message not in this channel or can't access
            continue

    if not target_message:
        return await status_msg.edit(content=f"❌ Message with ID {message_id} not found in any channel.")

    # Calculate how many messages to get before and after
    before_count = lines // 2
    after_count = lines - before_count

    # Get context messages
    context_messages = []

    # Try to use cached messages first
    cached_messages = await get_cached_messages(target_channel.id, limit=100)
    if cached_messages:
        # Find the target message index in the cached messages
        target_index = None
        for i, msg in enumerate(cached_messages):
            if msg.id == message_id:
                target_index = i
                break

        if target_index is not None:
            # Get context from cache when possible
            start_index = max(0, target_index - before_count)
            end_index = min(len(cached_messages), target_index + after_count + 1)
            context_messages = cached_messages[start_index:end_index]

    # If we couldn't get enough context from cache, fall back to API
    if not context_messages:
        # Get messages before target
        try:
            before_msgs = []
            async for msg in target_channel.history(limit=before_count, before=target_message):
                before_msgs.append(msg)
            # Reverse order to show oldest first
            before_msgs.reverse()
            context_messages.extend(before_msgs)
        except discord.HTTPException:
            pass

        # Add target message
        context_messages.append(target_message)

        # Get messages after target
        try:
            after_msgs = []
            async for msg in target_channel.history(limit=after_count, after=target_message):
                after_msgs.append(msg)
            context_messages.extend(after_msgs)
        except discord.HTTPException:
            pass

    # Add target message
    context_messages.append(target_message)

    # Get messages after target
    try:
        # Fix: Convert async generator to list manually instead of using flatten()
        after_msgs = []
        async for msg in target_channel.history(limit=after_count, after=target_message):
            after_msgs.append(msg)
        context_messages.extend(after_msgs)
    except discord.HTTPException:
        pass

    # Create the embed for displaying context
    embed = discord.Embed(
        title=f"Message Context in #{target_channel.name}",
        color=0x3498db,
        description=f"Context around [message]({target_message.jump_url}) from {target_message.author.name}"
    )

    # Format the messages
    context_content = ""
    for i, msg in enumerate(context_messages):
        timestamp = msg.created_at.strftime('%H:%M:%S')
        is_target = msg.id == message_id

        # Format the message differently if it's the target message
        if is_target:
            author_part = f"**→ {msg.author.name}**"
        else:
            author_part = f"{msg.author.name}"

        # Truncate long messages
        content = msg.content if len(msg.content) <= 300 else f"{msg.content[:297]}..."

        # Add message to context string
        context_content += f"[{timestamp}] {author_part}: {content}\n"

        # Add attachments if any
        if msg.attachments:
            attachment_list = ", ".join([f"[{a.filename}]({a.url})" for a in msg.attachments])
            context_content += f"📎 {attachment_list}\n"

        # Add message separator
        context_content += "\n"

    # Add context to embed
    embed.description = f"Context around [message]({target_message.jump_url}) from {target_message.author.name}\n\n{context_content}"

    # Add footer with navigation help
    embed.set_footer(text=f"Use !context {message_id} [lines] to adjust context size")

    await status_msg.edit(content=None, embed=embed)

    # Add direct jump link as a separate message for easy clicking
    await ctx.send(f"🔗 **Direct link to message:** {target_message.jump_url}")


@bot.command(name="regex", aliases=["regexsearch", "regexsearcher", "regsea", "rs", "rsearch", "reg"])
async def regex_search(ctx, *args):
    if not is_admin(ctx):
        return await ctx.send("❌ You must be a server admin to use this.")

    global search_cancelled, search_cooldowns

    # Handle cancel request
    if len(args) == 1 and args[0].lower() == "cancel":
        if hasattr(regex_search, "is_running") and regex_search.is_running:
            search_cancelled = True
            return await ctx.send("⚠️ Search cancelled.")
        else:
            return await ctx.send("⚠️ No search is currently running.")

    if not hasattr(regex_search, "is_running"):
        regex_search.is_running = False

    if regex_search.is_running:
        return await ctx.send("⚠️ A regex search is already running. Please wait for it to complete or use `!regex cancel` to stop it.")

    search_cancelled = False

    # Use the unified argument parsing function
    processed_args, flags = parse_command_args(args)

    # Extract flags with default values
    deep_search = flags.get("deep_search", False)
    query_limit = flags.get("query_limit", 500)  # Default limit
    custom_query = "query_limit" in flags
    include_channels = flags.get("include_channels", [])
    exclude_channels = flags.get("exclude_channels", [])

    # Check if there was an error in parsing arguments
    if "error" in flags:
        return await ctx.send(f"⚠️ {flags['error']}")

    # Check for required user and pattern
    if len(processed_args) < 2:
        return await ctx.send("⚠️ Usage: `!regex @user pattern [options]`\nExample: `!regex @user \"\\b\\w+ing\\b\"`")

    # Extract user
    try:
        user_arg = processed_args[0]
        if user_arg.startswith("<@") and user_arg.endswith(">"):
            user_id = user_arg[2:-1]
            if user_id.startswith("!"):
                user_id = user_id[1:]
        else:
            user_id = user_arg

        user_id = int(user_id)
        user = await get_cached_user(user_id)
        if not user:
            return await ctx.send("⚠️ User not found. Check if the ID is correct.")

        # Rest of args is the pattern
        regex_pattern = " ".join(processed_args[1:])

    except Exception as e:
        return await ctx.send(f"⚠️ Error: {e}\nUse @mention or user ID.")

    # Compile regex pattern
    try:
        pattern = re.compile(regex_pattern, re.IGNORECASE)
    except re.error as e:
        return await ctx.send(f"⚠️ Invalid regex pattern: {e}")

    # Check cooldown for deep searches
    if deep_search or custom_query:
        search_stats["deep_searches"] += 1
        guild_id = ctx.guild.id
        current_time = datetime.now()
        if guild_id in search_cooldowns:
            time_diff = (current_time - search_cooldowns[guild_id]).total_seconds()
            if time_diff < 600:  # 10 minutes cooldown
                remaining = int(600 - time_diff)
                minutes = remaining // 60
                seconds = remaining % 60
                return await ctx.send(f"⚠️ Deep search cooldown! Please wait {minutes}m {seconds}s before running another deep search.")
        search_cooldowns[guild_id] = current_time

    regex_search.is_running = True

    try:
        # Status message based on search type
        search_msg_prefix = ""
        if include_channels:
            channel_names = []
            for ch_id in include_channels:
                channel = discord.utils.get(ctx.guild.channels, name=ch_id.strip('#'))
                if channel:
                    channel_names.append(f"#{channel.name}")
            search_msg_prefix = f"in {', '.join(channel_names)} " if channel_names else ""
        elif exclude_channels:
            channel_names = []
            for ch_id in exclude_channels:
                channel = discord.utils.get(ctx.guild.channels, name=ch_id.strip('#'))
                if channel:
                    channel_names.append(f"#{channel.name}")
            search_msg_prefix = f"excluding {', '.join(channel_names)} " if channel_names else ""

        # Fix: Capitalize first letter when not deep searching
        searching_text = "Searching" if not deep_search else "Deep searching"
        status_msg = await ctx.send(
            f"🔍 {searching_text} {search_msg_prefix}for messages from {user.name} matching `{regex_pattern}`. This may take a while..."
        )

        # Prepare search channels
        search_channels = []
        if include_channels:
            for ch_name in include_channels:
                ch_name = ch_name.strip('#')
                channel = discord.utils.get(ctx.guild.text_channels, name=ch_name)
                if channel:
                    search_channels.append(channel)
        else:
            if exclude_channels:
                exclude_ch_names = [ch.strip('#') for ch in exclude_channels]
                search_channels = [ch for ch in ctx.guild.text_channels
                                   if ch.name not in exclude_ch_names]
            else:
                search_channels = ctx.guild.text_channels

        total_channels = len(search_channels)
        found_messages = []
        total_searched = 0
        start_time = datetime.now()
        last_update_time = start_time
        channels_searched = 0

        # Search through channels
        for channel in search_channels:
            channels_searched += 1

            # Check if search was cancelled
            if search_cancelled:
                await status_msg.edit(content="⚠️ Search cancelled.")
                search_stats["cancelled_searches"] += 1
                return

            try:
                messages = await get_cached_messages(channel.id, limit=query_limit, force_refresh=deep_search)
                for msg in messages:
                    if msg.author.id == user.id and pattern.search(msg.content):
                        found_messages.append((msg, channel))
                    total_searched += 1

                    # Update status message every 30 seconds to show progress
                    current_time = datetime.now()
                    if (current_time - last_update_time).total_seconds() > 30:
                        progress = int(channels_searched / total_channels * 100)
                        await status_msg.edit(content=f"🔍 {'Deep ' if deep_search else ''}searching {search_msg_prefix}for messages from {user.name} matching `{regex_pattern}`... {progress}% ({channels_searched}/{total_channels} channels, {total_searched:,} messages checked)")
                        last_update_time = current_time

            except discord.Forbidden:
                continue

        # Calculate search time
        search_time = (datetime.now() - start_time).total_seconds()

        if not found_messages:
            await status_msg.edit(
                content=f"❌ No messages found from {user.name} matching '{regex_pattern}'. Searched {total_searched:,} messages in {search_time:.1f}s."
            )
        else:
            # Format results
            result = f"✅ Found {len(found_messages)} regex matches for pattern `{regex_pattern}` from {user.name} (searched {total_searched:,} messages in {search_time:.1f}s):\n\n"

            for i, (msg, channel) in enumerate(found_messages, 1):
                timestamp = msg.created_at.strftime('%Y-%m-%d %H:%M:%S')
                content = msg.content if len(msg.content) <= 500 else f"{msg.content[:497]}..."
                result += f"{i}. **#{channel.name}** ({timestamp}):\n{content}\n[Jump to message]({msg.jump_url})\n\n"
                if len(result) > 1800:
                    await ctx.send(result)
                    result = ""
            if result:
                await ctx.send(result)
            await status_msg.edit(content=f"✅ Found {len(found_messages)} messages from {user.name} matching '{regex_pattern}'.")

        # Update global search stats
        search_stats["total_searches"] += 1
        search_stats["total_messages_searched"] += total_searched
        search_stats["total_matches_found"] += len(found_messages)
        search_stats["search_time_total"] += search_time

        # Update guild stats
        guild_name = ctx.guild.name
        if guild_name not in search_stats["searches_by_guild"]:
            search_stats["searches_by_guild"][guild_name] = 0
        search_stats["searches_by_guild"][guild_name] += 1

        # Update user stats
        user_name = f"{ctx.author.name}"
        if user_name not in search_stats["searches_by_user"]:
            search_stats["searches_by_user"][user_name] = 0
        search_stats["searches_by_user"][user_name] += 1

        # Update last search data
        search_stats["last_search"] = {
            "user": user.name,
            "keyword": regex_pattern,
            "messages": total_searched,
            "time": search_time,
            "matches": len(found_messages),
            "guild": ctx.guild.name
        }

        # Update largest search if applicable
        if total_searched > search_stats["largest_search"]["messages"]:
            search_stats["largest_search"] = {
                "messages": total_searched,
                "time": search_time,
                "keyword": regex_pattern,
                "guild": ctx.guild.name
            }

        save_search_stats()

    finally:
        regex_search.is_running = False
        search_cancelled = False


@bot.command(name="export")
async def export_results(ctx, *args):
    """Export search results to a file"""
    global search_cancelled, search_cooldowns

    if not is_admin(ctx):
        return await ctx.send("❌ You must be a server admin to use this.")

    # Handle cancel request
    if len(args) == 1 and args[0].lower() == "cancel":
        if hasattr(export_results, "is_running") and export_results.is_running:
            search_cancelled = True
            return await ctx.send("⚠️ Export cancelled.")
        else:
            return await ctx.send("⚠️ No export is currently running.")

    if not hasattr(export_results, "is_running"):
        export_results.is_running = False

    if export_results.is_running:
        return await ctx.send("⚠️ An export is already running. Please wait for it to complete or use `!export cancel` to stop it.")

    search_cancelled = False

    # Use the unified argument parsing function
    processed_args, flags = parse_command_args(args)

    # Extract flags with default values
    deep_search = flags.get("deep_search", False)
    query_limit = flags.get("query_limit", 500)  # Default limit
    custom_query = "query_limit" in flags
    include_channels = flags.get("include_channels", [])
    exclude_channels = flags.get("exclude_channels", [])

    # Check if there was an error in parsing arguments
    if "error" in flags:
        return await ctx.send(f"⚠️ {flags['error']}")

    # Check for required user and keyword arguments
    if len(processed_args) < 2:
        return await ctx.send("⚠️ Usage: `!export @user keyword [options]`")

    # Extract user from the first processed argument
    try:
        user_arg = processed_args[0]
        if user_arg.startswith("<@") and user_arg.endswith(">"):
            user_id = user_arg[2:-1]
            if user_id.startswith("!"):
                user_id = user_id[1:]
        else:
            user_id = user_arg

        user_id = int(user_id)
        user = await get_cached_user(user_id)
        if not user:
            return await ctx.send("⚠️ User not found. Check if the ID is correct.")

        # Extract keyword from remaining processed arguments
        keyword = " ".join(processed_args[1:])
    except Exception:
        return await ctx.send("⚠️ Invalid user format. Use @mention or user ID.")

    # Create exports directory if it doesn't exist
    os.makedirs("exports", exist_ok=True)

    filename = f"exports/{user.name}_{keyword.replace(' ', '_')[:20]}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"

    # Apply cooldown check for deep searches
    if deep_search or custom_query:
        # Same cooldown logic as in search_messages
        guild_id = ctx.guild.id
        current_time = datetime.now()
        if guild_id in search_cooldowns:
            time_diff = (current_time - search_cooldowns[guild_id]).total_seconds()
            if time_diff < 600:  # 10 minutes cooldown
                remaining = int(600 - time_diff)
                minutes = remaining // 60
                seconds = remaining % 60
                return await ctx.send(f"⚠️ Deep search cooldown! Please wait {minutes}m {seconds}s before running another deep search.")
        search_cooldowns[guild_id] = current_time

    # Fix: Capitalize first letter when not deep searching
    searching_text = "Searching" if not deep_search else "Deep searching"
    status_msg = await ctx.send(
        f"🔍 {searching_text} for messages from {user.name} containing '{keyword}'... Results will be exported to a file."
    )

    export_results.is_running = True

    try:
        # Prepare search channels
        search_channels = []
        if include_channels:
            for ch_name in include_channels:
                ch_name = ch_name.strip('#')
                channel = discord.utils.get(ctx.guild.text_channels, name=ch_name)
                if channel:
                    search_channels.append(channel)
        else:
            if exclude_channels:
                exclude_ch_names = [ch.strip('#') for ch in exclude_channels]
                search_channels = [ch for ch in ctx.guild.text_channels
                                  if ch.name not in exclude_ch_names]
            else:
                search_channels = ctx.guild.text_channels

        found_messages = []
        total_searched = 0
        start_time = datetime.now()
        last_update_time = start_time
        channels_searched = 0

        # Search through channels
        for channel in search_channels:
            channels_searched += 1

            # Check if search was cancelled
            if search_cancelled:
                await status_msg.edit(content=f"⚠️ Export cancelled after searching {total_searched:,} messages.")
                return

            # Update status message every 5 seconds
            current_time = datetime.now()
            if (current_time - last_update_time).total_seconds() > 5:
                progress = channels_searched / len(search_channels) * 100
                time_elapsed = (current_time - start_time).total_seconds()
                await status_msg.edit(content=f"🔍 {searching_text}... ({channels_searched}/{len(search_channels)} channels, {total_searched:,} messages, {progress:.1f}%, {time_elapsed:.1f}s)")
                last_update_time = current_time

            try:
                messages = []
                if deep_search or custom_query:
                    # Use specified limit for deep searches
                    async for msg in channel.history(limit=query_limit):
                        messages.append(msg)
                        total_searched += 1
                else:
                    # Use default limit for regular searches
                    async for msg in channel.history(limit=100):
                        messages.append(msg)
                        total_searched += 1

                # Search messages
                for msg in messages:
                    if msg.author.id == user.id and keyword.lower() in msg.content.lower():
                        found_messages.append((msg, channel))

            except discord.Forbidden:
                continue

        # Calculate search time
        search_time = (datetime.now() - start_time).total_seconds()

        # Export results to file
        with open(filename, "w", encoding="utf-8") as f:
            f.write(f"Export of messages from {user.name} containing '{keyword}'\n")
            f.write(f"Searched {total_searched:,} messages in {search_time:.1f}s\n")
            f.write(f"Found {len(found_messages)} matching messages\n")
            f.write(f"Export date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write("=" * 80 + "\n\n")

            if not found_messages:
                f.write("No matching messages found.")
            else:
                for i, (msg, channel) in enumerate(found_messages, 1):
                    timestamp = msg.created_at.strftime('%Y-%m-%d %H:%M:%S')
                    f.write(f"Message {i}/{len(found_messages)}\n")
                    f.write(f"Channel: #{channel.name}\n")
                    f.write(f"Date: {timestamp}\n")
                    f.write(f"Link: {msg.jump_url}\n")
                    f.write(f"Content: {msg.content}\n")

                    # Add attachments info
                    if msg.attachments:
                        f.write("Attachments:\n")
                        for a in msg.attachments:
                            f.write(f"  - {a.filename}: {a.url}\n")

                    f.write("\n" + "-" * 40 + "\n\n")

        # Update status and send file
        await status_msg.edit(content=f"✅ Export complete! Found {len(found_messages)} messages from {user.name} containing '{keyword}' (searched {total_searched:,} messages in {search_time:.1f}s)")

        # Send the file
        file = discord.File(filename, filename=os.path.basename(filename))
        await ctx.send(f"📁 Results file:", file=file)

        # Update global search stats
        search_stats["total_searches"] += 1
        search_stats["total_messages_searched"] += total_searched
        search_stats["total_matches_found"] += len(found_messages)
        search_stats["search_time_total"] += search_time

        # Update guild stats
        guild_name = ctx.guild.name
        if guild_name not in search_stats["searches_by_guild"]:
            search_stats["searches_by_guild"][guild_name] = 0
        search_stats["searches_by_guild"][guild_name] += 1

        # Update user stats
        user_name = f"{ctx.author.name}"
        if user_name not in search_stats["searches_by_user"]:
            search_stats["searches_by_user"][user_name] = 0
        search_stats["searches_by_user"][user_name] += 1

        # Update last search data
        search_stats["last_search"] = {
            "user": user.name,
            "keyword": keyword,
            "messages": total_searched,
            "time": search_time,
            "matches": len(found_messages),
            "guild": ctx.guild.name
        }

        # Update largest search if applicable
        if total_searched > search_stats["largest_search"]["messages"]:
            search_stats["largest_search"] = {
                "messages": total_searched,
                "time": search_time,
                "keyword": keyword,
                "guild": ctx.guild.name
            }

        save_search_stats()

    except Exception as e:
        await ctx.send(f"⚠️ Error during export: {e}")
    finally:
        export_results.is_running = False
        search_cancelled = False


@bot.command(name="help")
async def help_command(ctx):
    if not is_admin(ctx):
        return
    embed = discord.Embed(
        title="🛠️ Bot Commands",
        color=0x3498db,
        description="Available admin-only commands:"
    )
    embed.add_field(name="!setkeywords word1, word2...", value="Update search keywords", inline=False)
    embed.add_field(name="!toggleprints user/message", value="Toggle printed output", inline=False)
    embed.add_field(name="!showkeywords", value="Display current keywords", inline=False)
    embed.add_field(name="!scan --users/--messages/--all",
                    value="Manually scan server for matches (requires one option)", inline=False)
    embed.add_field(name="!search @user keyword [options]",
                    value="Search user messages with options:\n"
                          "• `--a/--all` for deep search\n"
                          "• `--q limit` for custom message limit (e.g. `--q 10k`)\n"
                          "• `--in #channel1,#channel2` to search only specific channels\n"
                          "• `--exclude #channel3,#channel4` to skip specific channels", inline=False)
    embed.add_field(name="!search cancel", value="Cancel a running search operation", inline=False)
    embed.add_field(name="!searchstats", value="Display statistics about searches performed", inline=False)
    embed.add_field(name="!regex @user pattern [options]",
                    value="Search using regex patterns with the same options as !search", inline=False)
    embed.add_field(name="!export @user keyword [options]",
                    value="Export search results to a text file with the same options as !search", inline=False)
    embed.add_field(name="!context message_id [lines=5]", value="Get conversation context around a specific message",
                    inline=False)
    embed.add_field(name="!autoscan on/off", value="Enable/disable periodic auto-scanning", inline=False)
    embed.add_field(name="!scaninterval <minutes>", value="Set auto-scan interval in minutes", inline=False)
    embed.add_field(name="!clearlogs today/all", value="Delete logs for today or all logs", inline=False)
    await ctx.send(embed=embed)


# --- Scheduled tasks ---
@tasks.loop(seconds=3600)
async def auto_scan():
    print(f"🔄 Running scheduled auto-scan ({datetime.now().strftime('%Y-%m-%d %H:%M:%S')})")

    scan_count = 0
    message_count = 0

    for guild in bot.guilds:
        if not guild.chunked:
            await guild.chunk(cache=True)

        for member in get_cached_members(guild.id):
            name_fields = f"{member.name} {member.display_name}"
            if keyword_match(name_fields):
                entry = f"[AUTO-SCAN] {member} ({member.id}) in {guild.name}"
                user_logger.info(entry)
                scan_count += 1

        for channel in guild.text_channels:
            try:
                messages = await get_cached_messages(channel.id, limit=100, force_refresh=True)
                for msg in messages:
                    if keyword_match(msg.content):
                        entry = f"[AUTO-SCAN] {msg.author} in #{channel.name} ({msg.guild.name}) > {msg.content}"
                        msg_logger.info(entry)
                        message_count += 1
            except discord.Forbidden:
                continue

    next_scan_time = datetime.now() + timedelta(seconds=auto_scan.seconds)
    next_scan_str = next_scan_time.strftime('%Y-%m-%d %H:%M:%S')

    print(f"✅ Auto-scan complete! Found {scan_count} matching members and {message_count} messages.")
    print(f"⏰ Next auto-scan scheduled for: {next_scan_str} (in {format_time_interval(auto_scan.seconds / 60)})")


@auto_scan.before_loop
async def before_auto_scan():
    """Wait for the bot to be ready before starting auto-scan"""
    await bot.wait_until_ready()


@bot.command(name="autoscan")
async def toggle_auto_scan(ctx, enabled: str = None):
    if not is_admin(ctx):
        return await ctx.send("❌ You must be a server admin to use this.")

    if enabled is None:
        status = "enabled" if CONFIG.get("auto_scan_enabled", False) else "disabled"
        interval = CONFIG.get("auto_scan_interval_minutes", 60)
        return await ctx.send(f"🔄 Auto-scan is {status} (interval: {format_time_interval(interval)})")

    if enabled.lower() in ("on", "true", "yes", "1"):
        CONFIG["auto_scan_enabled"] = True
        save_config()
        update_scheduled_tasks()
        interval = CONFIG.get("auto_scan_interval_minutes", 60)
        await ctx.send(f"✅ Auto-scan enabled (every {format_time_interval(interval)})")
    elif enabled.lower() in ("off", "false", "no", "0"):
        CONFIG["auto_scan_enabled"] = False
        save_config()
        update_scheduled_tasks()
        await ctx.send("❌ Auto-scan disabled")
    else:
        await ctx.send("⚠️ Use 'on' or 'off'")


@bot.command(name="scaninterval")
async def set_scan_interval(ctx, minutes: str):
    if not is_admin(ctx):
        return await ctx.send("❌ You must be a server admin to use this.")

    try:
        interval = float(minutes)

        # Check minimum value
        if interval < 0.5:
            return await ctx.send("⚠️ Interval must be at least 0.5 minutes (30 seconds)")

        # Store the interval in minutes
        CONFIG["auto_scan_interval_minutes"] = interval
        save_config()

        # Update the task
        update_scheduled_tasks()

        # Provide feedback with formatted time
        await ctx.send(f"✅ Auto-scan interval set to {format_time_interval(interval)}")

    except ValueError:
        await ctx.send("⚠️ Please provide a valid number for the interval")


@bot.command(name="clearlogs")
async def clear_logs(ctx, scope: str = "today"):
    """Clear logs based on scope (today/all)"""
    if not is_admin(ctx):
        return await ctx.send("❌ You must be a server admin to use this.")

    try:
        global msg_logger, user_logger, msg_log_path, user_log_path

        if scope.lower() == "today":
            # Get current date folder
            current_date = datetime.now().strftime('%Y-%m-%d')
            # Find all folders matching today's date
            today_folders = [folder for folder in os.listdir(LOGS_DIR)
                             if folder.startswith(current_date)]

            if not today_folders:
                return await ctx.send("ℹ️ No logs found for today.")

            count = 0
            for folder in today_folders:
                folder_path = os.path.join(LOGS_DIR, folder)
                if os.path.isdir(folder_path):
                    for file in os.listdir(folder_path):
                        os.remove(os.path.join(folder_path, file))
                        count += 1
                    os.rmdir(folder_path)

            # Refresh logging to ensure we're using a new folder
            msg_logger, user_logger, msg_log_path, user_log_path = setup_logging()

            await ctx.send(f"✅ Cleared {count} log files from today's folders.")

        elif scope.lower() == "all":
            # Confirm deletion with reaction
            confirm_msg = await ctx.send("⚠️ Are you sure you want to delete ALL logs? React with ✅ to confirm.")
            await confirm_msg.add_reaction("✅")

            def check(reaction, user):
                return user == ctx.author and str(reaction.emoji) == "✅" and reaction.message.id == confirm_msg.id

            try:
                # Wait for confirmation
                await bot.wait_for('reaction_add', timeout=30.0, check=check)

                # Delete all log folders
                count = 0
                for item in os.listdir(LOGS_DIR):
                    item_path = os.path.join(LOGS_DIR, item)
                    if os.path.isdir(item_path):
                        for file in os.listdir(item_path):
                            os.remove(os.path.join(item_path, file))
                            count += 1
                        os.rmdir(item_path)

                # Refresh logging
                msg_logger, user_logger, msg_log_path, user_log_path = setup_logging()

                await ctx.send(f"✅ Cleared {count} log files from all folders.")

            except asyncio.TimeoutError:
                await ctx.send("❌ Confirmation timed out. Logs were not deleted.")
                try:
                    await confirm_msg.delete()
                except:
                    pass

        else:
            await ctx.send("⚠️ Invalid option. Use 'today' or 'all'.")

    except Exception as e:
        await ctx.send(f"❌ Error clearing logs: {str(e)}")


# --- Utility management commands ---
@bot.command(name="clearcache")
async def clear_cache(ctx):
    """Clear all cached data"""
    if not is_admin(ctx):
        return await ctx.send("❌ You must be a server admin to use this.")

    global member_cache, message_cache, user_cache, keyword_match_cache

    member_cache.clear()
    message_cache.clear()
    user_cache.clear()
    keyword_match_cache.clear()

    await ctx.send("✅ All caches cleared successfully.")


@bot.command(name="sysinfo", aliases=["sys", "info", "system", "botinfo", "bot", "sinfo", "si", "bi"])
async def memory_info(ctx):
    """Show system and memory usage statistics"""
    if not is_admin(ctx):
        return await ctx.send("❌ You must be a server admin to use this.")

    loading_msg = await ctx.send("⏳ Building system information embed, please wait...")

    try:
        # Get cache statistics using the utility function
        cache_stats = get_cache_stats(member_cache, message_cache, user_cache, keyword_match_cache)

        # Get memory usage
        process = psutil.Process()
        memory_usage = process.memory_info().rss / 1024 / 1024  # Convert to MB
        system_memory = psutil.virtual_memory()

        # Create embed
        embed = discord.Embed(
            title="🖥️ System Information",
            color=0x3498db,
            description="Bot and system resource usage stats"
        )

        # System information
        embed.add_field(
            name="System",
            value=(f"**OS:** {platform.system()} {platform.release()}\n"
                   f"**Python:** {platform.python_version()}\n"
                   f"**Uptime:** {str(timedelta(seconds=int(psutil.boot_time())))}\n"),
            inline=False
        )

        # Discord info
        embed.add_field(
            name="Discord",
            value=(f"**API Latency:** {round(bot.latency * 1000)}ms\n"
                   f"**Guilds:** {len(bot.guilds)}\n"
                   f"**Users:** {sum(len(guild.members) for guild in bot.guilds)}\n"),
            inline=True
        )

        # Memory usage
        embed.add_field(
            name="Memory",
            value=(f"**Bot Usage:** {memory_usage:.2f} MB\n"
                   f"**System:** {system_memory.percent}% used\n"
                   f"**Available:** {system_memory.available / 1024 / 1024 / 1024:.2f} GB free"),
            inline=True
        )

        # Cache statistics
        embed.add_field(
            name="Cache Statistics",
            value=(f"**Members:** {cache_stats['member_count']:,} in {cache_stats['member_guilds']} guilds\n"
                   f"**Messages:** {cache_stats['message_count']:,} in {cache_stats['message_entries']} channels\n"
                   f"**Users:** {cache_stats['user_count']:,} cached\n"
                   f"**Keyword Matches:** {cache_stats['keyword_matches']:,}"),
            inline=False
        )

        # Memory used by caches
        embed.add_field(
            name="Cache Memory Usage",
            value=(f"**Member Cache:** {cache_stats['sizes']['member_size']:.2f} KB\n"
                   f"**Message Cache:** {cache_stats['sizes']['message_size']:.2f} KB\n"
                   f"**User Cache:** {cache_stats['sizes']['user_size']:.2f} KB\n"
                   f"**Keyword Cache:** {cache_stats['sizes']['keyword_size']:.2f} KB\n"
                   f"**Total:** {cache_stats['sizes']['total_size']:.2f} KB"),
            inline=True
        )

        # Log file info
        embed.add_field(
            name="Logs",
            value=(f"**Directory:** `{os.path.dirname(msg_log_path)}`\n"
                   f"**Message Log:** `{os.path.basename(msg_log_path)}`\n"
                   f"**User Log:** `{os.path.basename(user_log_path)}`"),
            inline=True
        )

        # Auto-scan status
        auto_scan_status = "Enabled" if CONFIG.get("auto_scan_enabled", False) else "Disabled"
        auto_scan_interval = CONFIG.get("auto_scan_interval_minutes", 60)

        embed.add_field(
            name="Auto-Scan Status",
            value=(f"**Status:** {auto_scan_status}\n"
                   f"**Interval:** {format_time_interval(auto_scan_interval)}"),
            inline=False
        )

        # Search statistics
        if search_stats["total_searches"] > 0:
            avg_time = search_stats["search_time_total"] / search_stats["total_searches"]
            avg_messages = search_stats["total_messages_searched"] / search_stats["total_searches"]

            embed.add_field(
                name="Search Statistics",
                value=(f"**Total Searches:** {search_stats['total_searches']}\n"
                       f"**Deep Searches:** {search_stats.get('deep_searches', 0)}\n"
                       f"**Messages Searched:** {search_stats['total_messages_searched']:,}\n"
                       f"**Matches Found:** {search_stats['total_matches_found']:,}\n"
                       f"**Avg Search Time:** {avg_time:.2f}s\n"
                       f"**Avg Messages/Search:** {avg_messages:.1f}"),
                inline=False
            )

        # Set footer with timestamp
        embed.set_footer(text=f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

        await loading_msg.edit(content=None, embed=embed)

    except Exception as e:
        await loading_msg.edit(content=f"❌ Error generating system info: {str(e)}")


@bot.command(name="listcache", aliases=["cacheinfo", "cache"])
async def list_cache(ctx):
    """Display information about currently cached data"""
    if not is_admin(ctx):
        return await ctx.send("❌ You must be a server admin to use this.")

    embed = discord.Embed(title="🗂️ Cache Information", color=0x3498db)

    # Member cache information
    member_cache_text = f"**Guilds cached:** {len(member_cache)}\n"
    member_cache_text += f"**Total members cached:** {sum(len(members) for members in member_cache.values())}\n"
    embed.add_field(name="Member Cache", value=member_cache_text, inline=False)

    # Message cache information
    message_cache_text = f"**Channel entries:** {len(message_cache)}\n"
    message_cache_text += f"**Total messages cached:** {sum(len(messages) for messages in message_cache.values())}\n"
    embed.add_field(name="Message Cache", value=message_cache_text, inline=False)

    # User cache information
    user_cache_text = f"**Users cached:** {len(user_cache)}\n"
    embed.add_field(name="User Cache", value=user_cache_text, inline=False)

    await ctx.send(embed=embed)


try:
    bot.run(TOKEN)
except discord.LoginFailure:
    print("Error: Invalid token. Please check your .env file.")
except Exception as e:
    print(f"Error starting bot: {e}")
