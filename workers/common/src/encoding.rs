use async_compression::tokio::bufread::{GzipEncoder, ZstdEncoder};
use bytes::Bytes;
use futures::Stream;
use std::pin::Pin;
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
    reqwest::Body::wrap_stream(compress_to_stream(stream, codec))
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

        let compressed_stream = futures::stream::once(futures::future::ready(
            Ok::<Bytes, std::io::Error>(Bytes::from(compressed)),
        ));
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

        let compressed_stream = futures::stream::once(futures::future::ready(
            Ok::<Bytes, std::io::Error>(Bytes::from(compressed)),
        ));
        let reader = StreamReader::new(compressed_stream);
        let decoder = GzipDecoder::new(reader);
        let decompressed = collect_stream(ReaderStream::new(decoder)).await;
        assert_eq!(decompressed, data);
    }
}
