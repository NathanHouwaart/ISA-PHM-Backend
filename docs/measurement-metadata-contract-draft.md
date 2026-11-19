# Draft: measurement metadata interoperability

This draft records a proposed application contract, not a released ISA-PHM
standard. The Wizard now uses the shared draft unit and quantity catalogues,
and the Backend-to-Wrapper identifier path is implemented. The shared
Specification validator is not yet integrated, and portable custom-unit
support remains unfinished.

Each applicable sensor or sub-sensor has measurement metadata under its selected measurement protocol. Blank draft fields are unanswered. Export requires each field to be known, unknown, or not applicable.

## Sampling rate

For a known rate, use a positive numeric `Sampling Rate` parameter with a frequency unit. Preserve a stable unit identifier and a readable unit symbol; wrapper analysis must normalize supported frequencies to Hz. For unknown or not applicable, encode the parameter value as `Unknown` or `Not applicable`, respectively, with no unit. These reserved values are statuses, not numbers.

## Measured unit

For a known unit, supply the existing mapping `value: [symbol, ""]` and a `valueIdentifier` string on that mapping. The backend emits a `Measured Unit` parameter whose value is an ontology annotation containing the symbol and identifier as `termAccession`. The wrapper exposes these as `value` and `value_identifier`. Do not use the parameter's numeric-unit field for the measured unit itself. For unknown or not applicable, encode the parameter value as `Unknown` or `Not applicable`, respectively, without a `valueIdentifier`. These reserved values are statuses, not units. Legacy plain-text units remain readable.

## Compatibility and open decisions

- Existing free-text parameters must be preserved. Duplicate legacy and built-in parameters require explicit review; conflicting values must not be silently resolved.
- Parameter names above are proposed canonical names; a stable parameter identity and catalogue version strategy remain to be specified.
- The selected unit identifier, source, label, and catalogue version need a verified ISA representation. Do not infer a unit from its symbol alone.
- Backend serialization and Wrapper extraction have tests for catalogue
  identifiers and explicit statuses. ISA-tools model loading is not by itself
  ISA JSON Schema validation; separate schema checks do not establish full
  ISA-PHM conformance. Expand end-to-end coverage before finalizing the contract.

The installed ISA tools accepted and reloaded these examples. That result does not establish complete ISA-PHM conformance.
