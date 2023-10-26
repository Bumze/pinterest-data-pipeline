import requests
import json

invoke_url1 = "https://kd1wwg7hnj.execute-api.us-east-1.amazonaws.com/0e35b2767ae1Pinterest/topics/0e35b2767ae1.pin"
payload1 = json.dumps({
"records": [
        {
    "value": {"index": pin_result["index"], "unique_id": pin_result["unique_id"], "title": pin_result["title"], "description": pin_result["description"], "poster_name": pin_result["poster_name"], 
    "follower_count": pin_result["follower_count"], "tag_list": pin_result["tag_list"], "is_image_or_video": pin_result["is_image_or_video"], "image_src": pin_result["image_src"], "downloaded": pin_result["downloaded"], 
    "save_location": pin_result["save_location"], "category": pin_result["category"]}
        }
    ]
})

x = requests.post(url, json = payload1)

print(x)