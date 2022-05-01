CREATE TABLE IF NOT EXISTS instructions (
    id VARCHAR(36) NOT NULL,
    opcode VARCHAR(50) NOT NULL,
    operand LONGBLOB NOT NULL,
    result LONGBLOB NOT NULL,
    state VARCHAR(20) NOT NULL,
    error TEXT NOT NULL,
    create_time bigint(20) NOT NULL,
    update_time bigint(20) NOT NULL,
    finish_time bigint(20) NOT NULL,
    PRIMARY KEY(id),
    INDEX idx_create(create_time),
    INDEX idx_update(update_time),
    INDEX idx_finish(finish_time)
);

CREATE TABLE IF NOT EXISTS programs (
    id varchar(36) NOT NULL,
    code LONGTEXT NOT NULL,
    state VARCHAR(20) NOT NULL,
    error TEXT NOT NULL,
    processor TEXT NOT NULL,
    create_time bigint(20) NOT NULL,
    update_time bigint(20) NOT NULL,
    finish_time bigint(20) NOT NULL,
    PRIMARY KEY(id),
    INDEX idx_create(create_time),
    INDEX idx_update(update_time),
    INDEX idx_finish(finish_time)
);

CREATE TABLE IF NOT EXISTS schedulers (
    id VARCHAR(36) NOT NULL,
    programs TEXT NOT NULL,    
    create_time bigint(20) NOT NULL,
    update_time bigint(20) NOT NULL,
    finish_time bigint(20) NOT NULL,
    PRIMARY KEY(id),
    INDEX idx_create(create_time),
    INDEX idx_update(update_time),
    INDEX idx_finish(finish_time)
);
