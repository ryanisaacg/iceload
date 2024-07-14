function check(op: "get" | "insert" | "update" | "remove", path: {string}, user: string): boolean
    if op == "get" or op == "insert" then
        return true
    else
        return false
    end
end

return check
