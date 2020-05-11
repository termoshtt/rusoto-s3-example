use bytes::Bytes;
use futures::{FutureExt, TryStreamExt};
use rusoto_core::Region;
use rusoto_s3::*;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

struct Bucket {
    s3: S3Client,
    name: String,
}

impl Bucket {
    fn new(region: Region, name: &str) -> Self {
        Bucket {
            s3: S3Client::new(region),
            name: name.into(),
        }
    }

    async fn create(&self) {
        let req = CreateBucketRequest {
            bucket: self.name.clone(),
            ..Default::default()
        };
        self.s3
            .create_bucket(req)
            .await
            .expect("Failed to create S3 bucket");
    }

    async fn put(&self, remote_filename: &str, local_filename: &str) {
        let meta = std::fs::metadata(local_filename).unwrap();
        let read_stream = tokio::fs::read(local_filename.to_owned())
            .into_stream()
            .map_ok(Bytes::from);

        let req = PutObjectRequest {
            bucket: self.name.clone(),
            key: remote_filename.to_owned(),
            content_length: Some(meta.len() as i64),
            body: Some(StreamingBody::new(read_stream)),
            ..Default::default()
        };

        self.s3.put_object(req).await.expect("Couldn't PUT object");
    }

    async fn get(&self, remote_filename: &str, local_filename: &str) {
        let get_req = GetObjectRequest {
            bucket: self.name.clone(),
            key: remote_filename.to_owned(),
            ..Default::default()
        };
        let result = self
            .s3
            .get_object(get_req)
            .await
            .expect("Couldn't GET object");
        let mut stream = result.body.unwrap().into_async_read();
        let mut buf = vec![0_u8; 1024];
        let mut f = tokio::fs::File::create(local_filename)
            .await
            .expect("Cannot create a new file");
        while let Ok(n) = stream.read(&mut buf).await {
            if n == 0 {
                break;
            }
            f.write(&buf[..n]).await.expect("Writing to file failed");
        }
    }
}

#[tokio::main]
async fn main() {
    let bucket = Bucket::new(Region::UsEast1, "test-bucket");
    bucket.create().await;
    bucket.put("src/main.rs", "src/main.rs").await;
    bucket.get("src/main.rs", "downloaded.rs").await;
}
