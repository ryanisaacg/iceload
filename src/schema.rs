use crate::message::RefComponent;

// TODO: in the future the schema will be aware of the shape of the database,
// allowing it to construct much more efficient keys from refs
pub struct Schema(());

impl Schema {
    pub fn empty() -> Schema {
        Schema(())
    }

    pub fn encode_ref(&self, refs: &[RefComponent]) -> Vec<u8> {
        let mut encoded = Vec::new();

        for r in refs.iter() {
            let (ref_ty, string) = match r {
                RefComponent::Collection(value) => (RefTypeEncoded::Collection, value.as_str()),
                RefComponent::Document(value) => (RefTypeEncoded::Document, value.as_str()),
            };
            let bytes = string.as_bytes();
            let len = bytes.len();
            encoded.push(ref_ty as u8);
            encoded.extend(len.to_le_bytes());
            encoded.extend(bytes);
        }

        encoded
    }

    pub fn decode_ref(&self, encoded_ref: &[u8]) -> Vec<RefComponent> {
        let mut decoded = Vec::new();

        let mut idx = 0;
        while idx < encoded_ref.len() {
            let ref_ty = match encoded_ref[idx] {
                0 => RefTypeEncoded::Collection,
                1 => RefTypeEncoded::Document,
                _ => unreachable!(),
            };
            let mut str_len_bytes = [0u8; USIZE_LEN];
            let len_end = idx + USIZE_LEN + 1;
            str_len_bytes.copy_from_slice(&encoded_ref[idx + 1..len_end]);
            let str_len = usize::from_le_bytes(str_len_bytes);
            let str_bytes = &encoded_ref[len_end..len_end + str_len];
            let string = String::from_utf8(str_bytes.to_vec()).unwrap();
            let ref_component = match ref_ty {
                RefTypeEncoded::Collection => RefComponent::Collection(string),
                RefTypeEncoded::Document => RefComponent::Document(string),
            };
            decoded.push(ref_component);
            idx = len_end + str_len;
        }

        decoded
    }
}

const USIZE_LEN: usize = std::mem::size_of::<usize>();

#[repr(u8)]
enum RefTypeEncoded {
    Collection = 0,
    Document = 1,
}

#[cfg(test)]
mod tests {

    use crate::message::{Ref, RefComponent};

    use super::Schema;

    #[test]
    fn round_trip_ref() {
        let schema = Schema::empty();
        let r = Ref(vec![
            RefComponent::Collection("apple".to_string()),
            RefComponent::Document("banana".to_string()),
            RefComponent::Collection("cherry".to_string()),
            RefComponent::Document("date".to_string()),
            RefComponent::Document("elderberry".to_string()),
        ]);
        let encoded = schema.encode_ref(&r.0);
        let decoded = schema.decode_ref(&encoded);
        assert_eq!(r, Ref(decoded));
    }
}
