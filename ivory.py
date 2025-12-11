from mastodon import Mastodon
from dotenv import dotenv_values
from kafka import KafkaProducer
import json
import datetime

config = dotenv_values(".env")

def serialize_datetime(obj):
    if isinstance(obj, datetime.datetime):
        return obj.isoformat()
    raise TypeError("Type not serializable")


producer = KafkaProducer(bootstrap_servers=config['KAFKA_SERVER'], value_serializer=lambda v: json.dumps(v, default=serialize_datetime).encode('utf-8'))
mastodon = Mastodon(api_base_url=config['MASTODON_BASE_API'], access_token=config['MASTODON_AUTH_TOKEN'])
# print(mastodon.toot('tooting a new toot'))
# print(mastodon.statuses())
# print(mastodon.followed_tags())
def post_status(status):
    print('createing status', status)
    mastodon.status_post(visibility='direct', status=status)

for tags in mastodon.followed_tags():
    for each_status in mastodon.timeline_hashtag(hashtag=tags.name):
        temp_object = {}
        temp_object['url'] = each_status.url
        temp_object['account'] = each_status.account
        temp_object['content'] = each_status.content
        temp_object['createdAt'] = each_status.created_at
        temp_object['mediaAttachments'] = each_status.media_attachments
        try:
            producer.send(config['KAFKA_TOPIC'], temp_object)
            print(temp_object['url'])
        except Exception as e:
            print('error is ', e)
    # print(tags, '\n')

# for each_status in mastodon.timeline():
    # print(each_status, '\n')


# post_status('r2d2 says hello')
