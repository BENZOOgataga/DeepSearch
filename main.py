import discord
import os
import json
import shutil
from discord.ext import commands
from dotenv import load_dotenv
from datetime import datetime

# --- Load env and config ---
load_dotenv()
TOKEN = os.getenv("BOT_TOKEN")
CONFIG_FILE = "config.json"

with open(CONFIG_FILE, "r", encoding="utf-8") as f:
    CONFIG = json.load(f)

# --- Setup logs ---
LOGS_DIR = "logs"
if os.path.exists(LOGS_DIR):
    shutil.rmtree(LOGS_DIR)
os.makedirs(LOGS_DIR, exist_ok=True)

msg_log_path = os.path.join(LOGS_DIR, "message_logs.log")
user_log_path = os.path.join(LOGS_DIR, "user_logs.log")

# --- Init bot ---
intents = discord.Intents.default()
intents.members = True
intents.message_content = True
bot = commands.Bot(command_prefix="!", intents=intents, help_command=None)


# --- Utils ---
def save_config():
    with open(CONFIG_FILE, "w", encoding="utf-8") as f:
        json.dump(CONFIG, f, indent=2)


def log_to_file(path, content):
    with open(path, "a", encoding="utf-8") as f:
        f.write(content)


def keyword_match(text):
    return any(k.lower() in text.lower() for k in CONFIG["search_keywords"])


def is_admin(ctx):
    return ctx.author.guild_permissions.administrator


# --- Events ---
@bot.event
async def on_ready():
    print(f"✅ Logged in as {bot.user}")
    print("🔍 Keywords:", ', '.join(CONFIG["search_keywords"]))
    print("📁 Logs at logs/")
    print()

    for guild in bot.guilds:
        await guild.chunk()
        print(f"🔍 Scanning guild: {guild.name} ({guild.id})")
        for member in guild.members:
            name_fields = f"{member.name} {member.display_name}"
            if keyword_match(name_fields):
                entry = f"[USER] {member} ({member.id}) in {guild.name}\n"
                log_to_file(user_log_path, entry)
                if CONFIG["print_user_matches"]:
                    print(entry.strip())


@bot.event
async def on_message(msg):
    if msg.author.bot or not msg.guild:
        return
    if keyword_match(msg.content):
        timestamp = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        entry = f"[{timestamp}] {msg.author} in #{msg.channel} ({msg.guild.name})\n> {msg.content}\n\n"
        log_to_file(msg_log_path, entry)
        if CONFIG["print_message_matches"]:
            print(entry.strip())
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
    await ctx.send(embed=embed)


bot.run(TOKEN)
