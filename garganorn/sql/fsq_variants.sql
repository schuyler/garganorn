ALTER TABLE places ADD COLUMN variants
    STRUCT(name VARCHAR, type VARCHAR, language VARCHAR)[] DEFAULT [];
