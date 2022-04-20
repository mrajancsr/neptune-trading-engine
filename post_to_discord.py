import requests

discord_webhooks = {
    "kraken-signals": "https://discord.com/api/webhooks/953558958893846558/LvalilNbHSIMkZCNwS7PHtm4d3IDqxMXpRK-oJ8CnO9Yt__8PuBIQNkM4G28xTsqDwCt",
    "sbf-says": "https://discord.com/api/webhooks/953601821417562122/8gJl1tr2kypBvgQni-aS3HKZMDyFWIxwGvXYhAwgS2Yu6ac2PI2WCu2cdbxkts9yA9ms",
}


def post_to_discord(channel, message):
    if channel is not None:
        channel_url = discord_webhooks[channel]
        data = {"content": message}
        response = requests.post(channel_url, json=data)
    else:
        print("Discord webhook channel must be specified")
