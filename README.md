# s3-objects-expiry-checker

1. Setup your AWS profile in ~/.aws/credentials
2. Create .env file with these content: 

```
EXPIRE_THRESHOLD = 20
BUCKET_NAME = #bucketname
PROFILE_NAME = #profile_name
TOPIC_ARN = #topicarn
REGION = ap-southeast-1
S3_LIFECYCLE = #linktos3lifecycletab
```

3. `pip install -r requirements.txt`