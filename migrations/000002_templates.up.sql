CREATE TABLE templates (
    name           text PRIMARY KEY CHECK (name ~ '^[a-z0-9][a-z0-9_-]{0,63}$'),
    channel        text NOT NULL CHECK (channel IN ('sms', 'email', 'push')),
    is_active      boolean NOT NULL DEFAULT true,
    latest_version int NOT NULL CHECK (latest_version >= 1),
    created_at     timestamptz NOT NULL DEFAULT now(),
    updated_at     timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE template_versions (
    template_name      text NOT NULL REFERENCES templates (name),
    version            int NOT NULL CHECK (version >= 1),
    body_tpl           text NOT NULL CHECK (octet_length(body_tpl) <= 65536),
    variables          text[] NOT NULL DEFAULT '{}',
    optional_variables text[] NOT NULL DEFAULT '{}',
    created_at         timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (template_name, version)
);
