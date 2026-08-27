-- generate_schema_name.sql
-- ─────────────────────────
-- Overrides dbt's default schema naming behaviour.
--
-- By default dbt creates: {target.schema}_{custom_schema}
-- e.g.  analytics_analytics, analytics_staging — confusing!
--
-- This macro tells dbt: if a custom schema is set, use ONLY that name.
-- Result: models land in exactly the schema you named in dbt_project.yml
--   staging models  → staging schema
--   mart models     → analytics schema

{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
