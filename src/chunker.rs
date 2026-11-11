use fastcdc::v2020::FastCDC;

#[derive(Debug, Clone, Copy)]
pub struct ChunkerOptions {
    pub min_size: u32,
    pub avg_size: u32,
    pub max_size: u32,
}

impl Default for ChunkerOptions {
    fn default() -> Self {
        Self {
            min_size: 256 * 1024,
            avg_size: 1024 * 1024,
            max_size: 4 * 1024 * 1024,
        }
    }
}

#[derive(Debug, Clone)]
pub struct RawChunk {
    pub offset: usize,
    pub data: Vec<u8>,
}

pub fn chunk_bytes(data: &[u8], opts: ChunkerOptions) -> crate::error::Result<Vec<RawChunk>> {
    if opts.min_size < 64 || opts.avg_size < opts.min_size || opts.max_size < opts.avg_size {
        return Err(crate::error::DiceError::msg(
            "invalid chunker options: require min <= avg <= max",
        ));
    }
    if data.is_empty() {
        return Ok(Vec::new());
    }

    let chunker = FastCDC::new(data, opts.min_size, opts.avg_size, opts.max_size);
    let mut out = Vec::new();
    for chunk in chunker {
        let start = chunk.offset as usize;
        let end = start + chunk.length as usize;
        out.push(RawChunk {
            offset: start,
            data: data[start..end].to_vec(),
        });
    }
    Ok(out)
}
