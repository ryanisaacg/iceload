use std::collections::HashMap;

use thiserror::Error;

use crate::message::RefComponent;

// TODO: in the future the schema will be aware of the shape of the database,
// allowing it to construct much more efficient keys from refs
pub struct Schema(SchemaItem);

impl Schema {
    pub fn empty() -> Schema {
        Schema(SchemaItem::Document(HashMap::new()))
    }

    pub fn create(root: SchemaItem) -> Schema {
        Schema(root)
    }

    pub fn encode_ref(&self, refs: &[RefComponent]) -> Vec<u8> {
        let mut encoded = Vec::new();

        for component in refs.iter() {
            let bytes = component.as_bytes();
            let len = bytes.len();
            encoded.extend(len.to_le_bytes());
            encoded.extend(bytes);
        }

        encoded
    }

    pub fn decode_ref(&self, encoded_ref: &[u8]) -> Vec<RefComponent> {
        let mut decoded = Vec::new();

        let mut idx = 0;
        while idx < encoded_ref.len() {
            let mut str_len_bytes = [0u8; USIZE_LEN];
            let len_end = idx + USIZE_LEN;
            str_len_bytes.copy_from_slice(&encoded_ref[idx..len_end]);
            let str_len = usize::from_le_bytes(str_len_bytes);
            let str_bytes = &encoded_ref[len_end..len_end + str_len];
            let string = String::from_utf8(str_bytes.to_vec()).unwrap();
            decoded.push(string);
            idx = len_end + str_len;
        }

        decoded
    }

    pub fn resolve(&self, refs: &[RefComponent]) -> Result<&SchemaItem, SchemaResolutionError> {
        self.0.resolve(refs)
    }
}

#[derive(Debug, Error)]
pub enum SchemaResolutionError {
    #[error("unknown field: {}", .0)]
    UnknownField(String),
    #[error("path continues through scalar value")]
    IllegalRefOnScalar,
}

#[derive(Debug)]
pub enum SchemaItem {
    Collection(Box<SchemaItem>),
    Document(HashMap<String, SchemaItem>),
    Scalar,
}

impl SchemaItem {
    fn resolve(&self, refs: &[RefComponent]) -> Result<&SchemaItem, SchemaResolutionError> {
        if refs.is_empty() {
            Ok(self)
        } else {
            match self {
                SchemaItem::Collection(inner) => inner.resolve(&refs[1..]),
                SchemaItem::Document(fields) => fields
                    .get(&refs[0])
                    .ok_or_else(|| SchemaResolutionError::UnknownField(refs[0].clone()))?
                    .resolve(&refs[1..]),
                SchemaItem::Scalar => Err(SchemaResolutionError::IllegalRefOnScalar),
            }
        }
    }
}

const USIZE_LEN: usize = std::mem::size_of::<usize>();

#[cfg(test)]
mod tests {

    use crate::message::Ref;

    use super::Schema;

    #[test]
    fn round_trip_ref() {
        let schema = Schema::empty();
        let r = Ref(vec![
            "apple".to_string(),
            "banana".to_string(),
            "cherry".to_string(),
            "date".to_string(),
            "elderberry".to_string(),
        ]);
        let encoded = schema.encode_ref(&r.0);
        let decoded = schema.decode_ref(&encoded);
        assert_eq!(r, Ref(decoded));
    }
}
