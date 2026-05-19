use std::io::{Read, Write};

pub fn compress(input: &[u8], quality: u32, lgwin: u32) -> std::io::Result<Vec<u8>> {
    let mut output = Vec::new();
    {
        let mut compressor = brotli::CompressorWriter::new(&mut output, 4096, quality, lgwin);
        compressor.write_all(input)?;
    }
    Ok(output)
}

pub fn decompress(input: &[u8]) -> std::io::Result<Vec<u8>> {
    let mut decompressor = brotli::Decompressor::new(input, 4096);
    let mut output = Vec::new();
    decompressor.read_to_end(&mut output)?;
    Ok(output)
}
