# utils/command_utils.py
from datetime import datetime


def setup_command_execution(command_function):
    """Set up initial command execution state and return it if already running"""
    if not hasattr(command_function, "is_running"):
        command_function.is_running = False

    if command_function.is_running:
        return False

    command_function.is_running = True
    return True


def handle_cancel_request(command_function, args):
    """Handle cancellation request for a command"""
    if len(args) == 1 and args[0].lower() == "cancel":
        if hasattr(command_function, "is_running") and command_function.is_running:
            return True
        return False
    return None


def apply_cooldown(search_cooldowns, ctx, deep_search, custom_query, cooldown_minutes=5):
    """Apply cooldown for intensive searches"""
    if deep_search or custom_query:
        guild_id = ctx.guild.id
        current_time = datetime.now()

        if guild_id in search_cooldowns:
            last_search = search_cooldowns[guild_id]
            elapsed = (current_time - last_search).total_seconds() / 60

            # If cooldown hasn't expired yet
            if elapsed < cooldown_minutes:
                remaining = cooldown_minutes - elapsed
                return False, remaining

        search_cooldowns[guild_id] = current_time

    return True, 0
