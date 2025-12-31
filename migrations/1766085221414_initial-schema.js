/**
 * @type {import('node-pg-migrate').ColumnDefinitions | undefined}
 */
export const shorthands = undefined;

/**
 * @param pgm {import('node-pg-migrate').MigrationBuilder}
 * @param run {() => void | undefined}
 * @returns {Promise<void> | void}
 */
export const up = (pgm) => {
  pgm.createTable('hosts', {
    id: 'id',
    name: { type: 'varchar(128)', notNull: true },
    email: { type: 'varchar(128)', notNull: true, unique: true }
  });
  pgm.createTable('guests', {
    id: 'id',
    name: { type: 'varchar(128)', notNull: true },
    email: { type: 'varchar(128)', notNull: false, unique: true },
    phone: { type: 'varchar(64)', notNull: false }
  });
  pgm.createTable('parties', {
    id: 'id',
    title: { type: 'varchar(128)', notNull: true },
    details: { type: 'varchar(2048)', notNull: false },
    scheduled_for: { type: 'datetime', notNull: true },
    state: { type: 'varchar(32)', notNull: true, default: 'draft' }
  });
  pgm.createTable('invites', {
    id: 'id',
    guest_id: { type: 'integer', notNull: true, references: 'guests', onDelete: 'CASCADE' },
    party_id: { type: 'integer', notNull: true, references: 'parties', onDelete: 'CASCADE' },
    state: { type: 'varchar(32)', notNull: true, default: 'draft' }
  });
  pgm.createTable('channels', {
    id: 'id',
    name: { type: 'varchar(128)', notNull: true },
    description: { type: 'varchar(2048)', notNull: false },
    scheduled_for: { type: 'datetime', notNull: true },
    type: { type: 'varchar(32)', notNull: true, default: 'chat' }
  });
  pgm.createTable('channel_memberships', {
    id: 'id',
    invite_id: { type: 'integer', notNull: true, references: 'invites', onDelete: 'CASCADE' },
    channel_id: { type: 'integer', notNull: true, references: 'channels', onDelete: 'CASCADE' }
  });
  pgm.createTable('channel_messages', {
    id: 'id',
    channel_id: { type: 'integer', notNull: true, references: 'channels', onDelete: 'CASCADE' },
    content: { type: 'varchar(2048)', notNull: true },
    timestamp: { type: 'datetime', notNull: true }
  });
};

/**
 * @param pgm {import('node-pg-migrate').MigrationBuilder}
 * @param run {() => void | undefined}
 * @returns {Promise<void> | void}
 */
export const down = (pgm) => {
  pgm.dropTable('hosts');
  pgm.dropTable('guests');
  pgm.dropTable('parties');
  pgm.dropTable('invites');
  pgm.dropTable('channels');
  pgm.dropTable('channel_memberships');
  pgm.dropTable('channel_messages');
};
