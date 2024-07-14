use mlua::{Compiler, Function, Lua};
use thiserror::Error;

use crate::message::Ref;

#[derive(Debug, Error)]
pub enum PermissionError {
    #[error("lua error: {}", .0)]
    LuaError(#[from] mlua::Error),
}

pub struct Permissions<'a> {
    lua: Lua,
    bytecode: &'a [u8],
}

impl Permissions<'_> {
    pub fn load_bytecode(permission_source: &str) -> Result<&'static [u8], PermissionError> {
        let lua_compiler = Compiler::new();
        let permission_function = lua_compiler.compile(permission_source).leak() as &'static [u8];

        let lua = Lua::new();
        // Double check script compiles
        lua.load(permission_function).eval()?;

        Ok(permission_function)
    }

    pub fn new(bytecode: &[u8]) -> Permissions<'_> {
        Permissions {
            lua: Lua::new(),
            bytecode,
        }
    }

    pub fn check(&self, op: Operation, _path: &Ref) -> Result<bool, PermissionError> {
        let func: Function = self.lua.load(self.bytecode).eval()?;
        // TODO: pass down path
        // TODO: pass down user ID
        let result: bool = func.call(match op {
            Operation::Read => "read",
            Operation::Insert => "insert",
            Operation::Update => "update",
            Operation::Remove => "remove",
        })?;

        Ok(result)
    }
}

pub enum Operation {
    Read,
    Insert,
    Update,
    Remove,
}
