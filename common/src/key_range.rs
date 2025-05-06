use bytes::Bytes;

#[derive(Clone, Debug, PartialEq)]
pub struct KeyRange {
    pub lower_bound_inclusive: Option<Bytes>,
    pub upper_bound_exclusive: Option<Bytes>,
}

impl KeyRange {
    pub fn includes(&self, key: Bytes) -> bool {
        let within_lower = match &self.lower_bound_inclusive {
            None => true,
            Some(b) => key >= b,
        };
        let within_upper = match &self.upper_bound_exclusive {
            None => true,
            Some(b) => key < b,
        };
        within_lower && within_upper
    }

    pub fn all() -> KeyRange {
        KeyRange {
            lower_bound_inclusive: None,
            upper_bound_exclusive: None,
        }
    }

    pub fn empty() -> KeyRange {
        KeyRange {
            lower_bound_inclusive: Some(Bytes::new()),
            upper_bound_exclusive: Some(Bytes::new()),
        }
    }
}

impl From<&proto::universe::KeyRange> for KeyRange {
    fn from(key_range: &proto::universe::KeyRange) -> Self {
        KeyRange {
            lower_bound_inclusive: Some(Bytes::from(key_range.lower_bound_inclusive.clone())),
            upper_bound_exclusive: Some(Bytes::from(key_range.upper_bound_exclusive.clone())),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn basic_test() {
        let key = Bytes::from_static(b"A");
        assert!(!KeyRange::empty().includes(key.clone()));
        assert!(KeyRange::all().includes(key.clone()));
        let real_range = KeyRange {
            lower_bound_inclusive: Some(Bytes::from_static(b"C")),
            upper_bound_exclusive: Some(Bytes::from_static(b"G")),
        };
        assert!(real_range.includes(Bytes::from_static(b"C")));
        assert!(real_range.includes(Bytes::from_static(b"DD")));
        assert!(!real_range.includes(Bytes::from_static(b"G")));
        assert!(!real_range.includes(Bytes::from_static(b"Z")));
    }
}
