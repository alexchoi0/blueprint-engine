bucket = "your-bucket-name"

result = gcp("gcs.upload", bucket=bucket, object="test/hello.txt", data="Hello from Blueprint3!")
print("Upload result:", result)

result = gcp("gcs.download", bucket=bucket, object="test/hello.txt")
print("Download result:", result)

result = gcp("gcs.list", bucket=bucket, prefix="test/")
print("List result:", result)

result = gcp("gcs.exists", bucket=bucket, object="test/hello.txt")
print("Exists result:", result)

result = gcp("gcs.get_metadata", bucket=bucket, object="test/hello.txt")
print("Metadata result:", result)

result = gcp("gcs.copy",
    src_bucket=bucket, src_object="test/hello.txt",
    dest_bucket=bucket, dest_object="test/hello-copy.txt")
print("Copy result:", result)

result = gcp("gcs.delete", bucket=bucket, object="test/hello-copy.txt")
print("Delete copy result:", result)

result = gcp("gcs.delete", bucket=bucket, object="test/hello.txt")
print("Delete original result:", result)
