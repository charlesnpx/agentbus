# Cursor protocol fixture provenance

`invalid-model-2026.09.18.json` is a sanitized, constructed response, not a
claimed wire capture. It is derived from the locally installed
`cursor-agent 2026.09.18-9a7762b` ACP implementation (`1006.index.js` in that
version's distribution). Its `unstable_setSessionModel` delegates to
`setSessionConfigOption`; `applyParameterizedModelConfigOption` and
`applyVariantModelConfigOption` call `invalidParams` with the message
`Invalid model value: ${value}` for an unknown model. An unauthenticated ACP
`authenticate` response from that same executable supplied the `-32602`,
`Invalid params`, and nested `data.message` envelope. The fixture substitutes
an illustrative ID and model. The direct-message test is a synthetic edge case.
An authenticated `session/set_model` wire capture was unavailable in the
isolated home used for local checks.
