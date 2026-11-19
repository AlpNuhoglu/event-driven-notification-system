-- name: InsertTemplate :one
-- Template create: no row returned means the name is taken (the store maps
-- pgx.ErrNoRows to ErrTemplateExists). Two concurrent creates of one name:
-- the second waits for the first to commit, then inserts nothing.
INSERT INTO templates (name, channel, latest_version)
VALUES (@name, @channel, 1)
ON CONFLICT (name) DO NOTHING
RETURNING name;

-- name: InsertTemplateVersion :exec
INSERT INTO template_versions (template_name, version, body_tpl, variables, optional_variables)
VALUES (@template_name, @version, @body_tpl, @variables, @optional_variables);

-- name: LockTemplate :one
-- Serialises the writers of one template's versions.
SELECT latest_version FROM templates WHERE name = @name FOR UPDATE;

-- name: BumpTemplateVersion :exec
UPDATE templates SET latest_version = @latest_version, updated_at = now() WHERE name = @name;

-- name: GetTemplateLatest :one
-- The template and its latest version, read in one statement.
SELECT t.name, t.channel, t.is_active, t.latest_version, t.created_at, t.updated_at,
       v.version, v.body_tpl, v.variables, v.optional_variables,
       v.created_at AS version_created_at
FROM templates t
JOIN template_versions v ON v.template_name = t.name AND v.version = t.latest_version
WHERE t.name = @name;

-- name: GetTemplateAtVersion :one
-- The template and the given version, read in one statement.
SELECT t.name, t.channel, t.is_active, t.latest_version, t.created_at, t.updated_at,
       v.version, v.body_tpl, v.variables, v.optional_variables,
       v.created_at AS version_created_at
FROM templates t
JOIN template_versions v ON v.template_name = t.name AND v.version = @version
WHERE t.name = @name;

-- name: TemplateExists :one
SELECT EXISTS (SELECT 1 FROM templates WHERE name = @name);

-- name: SetTemplateActive :execrows
-- Writes only when the value changes, so a repeat leaves updated_at alone.
UPDATE templates SET is_active = @is_active, updated_at = now()
WHERE name = @name AND is_active IS DISTINCT FROM @is_active;

-- name: ListTemplates :many
-- One page of the listing, by name in ASCII order (COLLATE "C", served by
-- templates_name_c_idx). @after is empty for the first page: every name sorts
-- after the empty string.
SELECT name, channel, is_active, latest_version, created_at, updated_at
FROM templates
WHERE name COLLATE "C" > @after::text
ORDER BY name COLLATE "C"
LIMIT @n;
