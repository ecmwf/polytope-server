use async_compression::tokio::bufread::{GzipEncoder, ZstdEncoder};
use bytes::Bytes;
use futures::{Stream, StreamExt};
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio_util::io::{ReaderStream, StreamReader};

use crate::delivery_config::Codec;

type BoxStream = Pin<Box<dyn Stream<Item = Result<Bytes, std::io::Error>> + Send + 'static>>;

fn compress_to_stream<S>(stream: S, codec: &Codec) -> BoxStream
where
    S: Stream<Item = Result<Bytes, std::io::Error>> + Send + Unpin + 'static,
{
    match codec {
        Codec::Identity => Box::pin(stream),
        Codec::Gzip => {
            let reader = StreamReader::new(stream);
            let encoder = GzipEncoder::new(reader);
            Box::pin(ReaderStream::new(encoder))
        }
        Codec::Zstd => {
            let reader = StreamReader::new(stream);
            let encoder = ZstdEncoder::new(reader);
            Box::pin(ReaderStream::new(encoder))
        }
    }
}

/// Wraps a byte stream with the given compression codec.
/// Returns the compressed stream as a reqwest::Body.
/// If the codec is Identity, the stream is passed through unchanged.
pub fn encode_stream<S>(stream: S, codec: &Codec) -> reqwest::Body
where
    S: Stream<Item = Result<Bytes, std::io::Error>> + Send + Unpin + 'static,
{
    encode_stream_counted(stream, codec).0
}

/// Like [`encode_stream`], but also returns a counter that tallies the number of
/// post-encoding bytes (i.e. the bytes actually delivered) as the body is
/// consumed downstream. The counter only reaches its final value once the
/// returned body has been fully streamed, so read it after delivery completes.
pub fn encode_stream_counted<S>(stream: S, codec: &Codec) -> (reqwest::Body, Arc<AtomicU64>)
where
    S: Stream<Item = Result<Bytes, std::io::Error>> + Send + Unpin + 'static,
{
    let counter = Arc::new(AtomicU64::new(0));
    let counter_clone = Arc::clone(&counter);
    let counted = compress_to_stream(stream, codec).inspect(move |item| {
        if let Ok(chunk) = item {
            counter_clone.fetch_add(chunk.len() as u64, Ordering::Relaxed);
        }
    });
    (reqwest::Body::wrap_stream(counted), counter)
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_compression::tokio::bufread::{GzipDecoder, ZstdDecoder};
    use futures::TryStreamExt;
    use tokio_util::io::{ReaderStream, StreamReader};

    fn make_stream(data: Vec<u8>) -> impl Stream<Item = Result<Bytes, std::io::Error>> + Unpin {
        futures::stream::once(futures::future::ready(Ok::<Bytes, std::io::Error>(
            Bytes::from(data),
        )))
    }

    async fn collect_stream<S>(stream: S) -> Vec<u8>
    where
        S: Stream<Item = Result<Bytes, std::io::Error>>,
    {
        futures::pin_mut!(stream);
        stream
            .try_fold(Vec::new(), |mut acc, chunk| async move {
                acc.extend_from_slice(&chunk);
                Ok(acc)
            })
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn identity_passthrough() {
        let data = b"hello world".to_vec();
        let stream = make_stream(data.clone());
        let result = collect_stream(compress_to_stream(stream, &Codec::Identity)).await;
        assert_eq!(result, data);
    }

    #[tokio::test]
    async fn zstd_roundtrip() {
        let data: Vec<u8> = (0..10_000).map(|i| (i % 256) as u8).collect();
        let stream = make_stream(data.clone());
        let compressed = collect_stream(compress_to_stream(stream, &Codec::Zstd)).await;

        let compressed_stream =
            futures::stream::once(futures::future::ready(Ok::<Bytes, std::io::Error>(
                Bytes::from(compressed),
            )));
        let reader = StreamReader::new(compressed_stream);
        let decoder = ZstdDecoder::new(reader);
        let decompressed = collect_stream(ReaderStream::new(decoder)).await;
        assert_eq!(decompressed, data);
    }

    #[tokio::test]
    async fn gzip_roundtrip() {
        let data: Vec<u8> = (0..10_000).map(|i| (i % 256) as u8).collect();
        let stream = make_stream(data.clone());
        let compressed = collect_stream(compress_to_stream(stream, &Codec::Gzip)).await;

        let compressed_stream =
            futures::stream::once(futures::future::ready(Ok::<Bytes, std::io::Error>(
                Bytes::from(compressed),
            )));
        let reader = StreamReader::new(compressed_stream);
        let decoder = GzipDecoder::new(reader);
        let decompressed = collect_stream(ReaderStream::new(decoder)).await;
        assert_eq!(decompressed, data);
    }

    #[tokio::test]
    async fn encode_stream_counted_tallies_identity_bytes() {
        use http_body_util::BodyExt;
        let data = vec![b'x'; 4096];
        let (body, counter) = encode_stream_counted(make_stream(data.clone()), &Codec::Identity);
        // Counter only settles once the body is fully consumed.
        assert_eq!(counter.load(Ordering::Relaxed), 0);
        let delivered = body.collect().await.unwrap().to_bytes();
        assert_eq!(delivered.len(), data.len());
        assert_eq!(counter.load(Ordering::Relaxed), data.len() as u64);
    }

    #[tokio::test]
    async fn encode_stream_counted_counts_post_encoding_bytes() {
        use http_body_util::BodyExt;
        let data = vec![b'a'; 10_000];
        let (body, counter) = encode_stream_counted(make_stream(data.clone()), &Codec::Gzip);
        let delivered = body.collect().await.unwrap().to_bytes();
        let counted = counter.load(Ordering::Relaxed);
        // The counter reflects bytes actually delivered (post-compression), not input size.
        assert_eq!(counted as usize, delivered.len());
        assert!(
            counted < data.len() as u64,
            "gzip of repetitive data should be smaller than the {}-byte input, got {counted}",
            data.len()
        );
    }
}
