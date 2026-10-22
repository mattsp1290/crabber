use std::collections::VecDeque;

pub(crate) struct Tail {
    bytes: VecDeque<u8>,
    #[cfg(any(unix, test))]
    capacity: usize,
    truncated: bool,
}

impl Tail {
    pub(crate) fn new(capacity: usize) -> Self {
        Self {
            bytes: VecDeque::with_capacity(capacity),
            #[cfg(any(unix, test))]
            capacity,
            truncated: false,
        }
    }

    #[cfg(any(unix, test))]
    pub(crate) fn write(&mut self, bytes: &[u8]) {
        if bytes.len() > self.capacity.saturating_sub(self.bytes.len()) {
            self.truncated = true;
        }
        if bytes.len() >= self.capacity {
            self.bytes.clear();
            self.bytes.extend(&bytes[bytes.len() - self.capacity..]);
        } else {
            let discard = (self.bytes.len() + bytes.len()).saturating_sub(self.capacity);
            self.bytes.drain(..discard);
            self.bytes.extend(bytes);
        }
    }

    #[cfg(any(unix, test))]
    pub(crate) fn mark_truncated(&mut self) {
        self.truncated = true;
    }

    pub(crate) fn snapshot(&self) -> (String, bool) {
        let bytes: Vec<_> = self.bytes.iter().copied().collect();
        (String::from_utf8_lossy(&bytes).into_owned(), self.truncated)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn tail_exact_suffix_and_displacement() {
        let mut tail = Tail::new(4);
        tail.write(b"abcd");
        assert_eq!(tail.snapshot(), ("abcd".into(), false));
        tail.write(b"ef");
        assert_eq!(tail.snapshot(), ("cdef".into(), true));
        tail.write(b"0123456789");
        assert_eq!(tail.snapshot(), ("6789".into(), true));
    }
    #[test]
    fn tail_lossy_cut_and_mark() {
        let mut tail = Tail::new(2);
        tail.write("€".as_bytes());
        assert_eq!(tail.snapshot(), ("��".into(), true));
        let mut tail = Tail::new(4);
        tail.mark_truncated();
        assert_eq!(tail.snapshot(), ("".into(), true));
    }
    #[test]
    fn tail_flood_stays_at_capacity() {
        let mut tail = Tail::new(4096);
        let bytes = [b'x'; 8192];
        for _ in 0..8192 {
            tail.write(&bytes);
        }
        assert_eq!(tail.bytes.len(), 4096);
        assert_eq!(tail.bytes.capacity(), 4096);
        assert!(tail.snapshot().1);
    }
}
