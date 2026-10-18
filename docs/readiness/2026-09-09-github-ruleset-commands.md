# NVIDIA Recovery: GitHub ruleset commands

> [!WARNING]
> Historical runbook for `NVIDIA/NVIDIA-Recovery`, verified on 2026-09-09.
> The captured repository, team, app and ruleset IDs and commands below are
> preserved as evidence. Do not run these commands for
> `dsx-ai-factory/NVIDIA-Recovery` or substitute only the repository name.
> Follow the [destination setup requirements](../governance/ci-approval.md#administrator-setup-and-acceptance)
> and inspect current destination settings before preparing any changes.

These commands configure GitHub repository rulesets and CI approval controls.
Run the numbered blocks in order in the **same Bash session**, using `gh` with
repository administration access and `jq`. The preparation blocks only read
GitHub and create local files. The explicitly labeled apply blocks change
GitHub. No apply command was executed while preparing this runbook.

Verified on 2026-09-09:

| Control | Current ID/state | Intended change |
| --- | --- | --- |
| Main Branch Protection | `22603728`, evaluate | Activate; remove creation/update restrictions; require existing CI and review controls. |
| Release Tag | `20813689`, active | Retain maintainer creation control; add a separate update/deletion rule with no bypass. |
| Copied PR branches | No repository ruleset found | Restrict `pull-request/*` writes to copy-pr-bot. |
| Maintainer team | `18776197` | `NVIDIA/nvidia-recovery-developers`; verify live before relying on it. |
| copy-pr-bot app | `354886` | App ID, not installation ID or bot user ID. |
| GitHub Actions | `15368` | Expected source for required language checks. |

Secret scanning, push-protection setup, credential cleanup and history rewriting
remain explicitly deferred. These commands do not configure those features.
Rulesets do not configure bot trustees/vetters or release-environment reviewers;
those remain separate Stage 0/PR1 tasks.

## 1. Back up and prepare

Run `bash` first if your terminal uses another shell. Stop on any failed command;
resolve the error before proceeding. The temporary directory contains no tokens.
Keep it until the settings have been verified.

```bash
set -euo pipefail

dsx_repo=NVIDIA/NVIDIA-Recovery
dsx_main_id=22603728
dsx_tag_id=20813689
dsx_rules_dir=$(mktemp -d "${TMPDIR:-/tmp}/nvidia-recovery-rules.XXXXXX")
printf 'Ruleset backup and payload directory: %s\n' "$dsx_rules_dir"

gh api "repos/$dsx_repo/rulesets/$dsx_main_id" > "$dsx_rules_dir/main.before.json"
gh api "repos/$dsx_repo/rulesets/$dsx_tag_id" > "$dsx_rules_dir/tags.before.json"
gh api --paginate --slurp "repos/$dsx_repo/rulesets?per_page=100" \
  | jq 'add' > "$dsx_rules_dir/rulesets.before.json"

dsx_team_id=$(gh api orgs/NVIDIA/teams/nvidia-recovery-developers --jq .id)
dsx_bot_app_id=$(gh api apps/copy-pr-bot --jq .id)

# Strip response-only metadata for PUT requests and comparisons.
cat > "$dsx_rules_dir/writable.jq" <<'JQ'
{name, target, enforcement, bypass_actors, conditions, rules}
JQ

jq -e '.target == "branch" and .source_type == "Repository" and
  .conditions.ref_name.include == ["~DEFAULT_BRANCH"] and
  (.bypass_actors | length == 0)' "$dsx_rules_dir/main.before.json"
jq -e --argjson team "$dsx_team_id" '.target == "tag" and
  .conditions.ref_name.include == ["refs/tags/v*"] and
  (.bypass_actors | length == 1) and
  any(.bypass_actors[]; .actor_type == "Team" and .actor_id == $team
    and .bypass_mode == "always")' "$dsx_rules_dir/tags.before.json"

jq -S -f "$dsx_rules_dir/writable.jq" "$dsx_rules_dir/main.before.json" \
  > "$dsx_rules_dir/main.restore.json"
jq -S -f "$dsx_rules_dir/writable.jq" "$dsx_rules_dir/tags.before.json" \
  > "$dsx_rules_dir/tags.restore.json"
```

If these assertions fail, the repository has changed since this review. Inspect
that change and adapt the payload rather than discarding newer controls.

## 2. Prepare the main-branch payload

This preserves existing rule parameters, signatures, linear-history policy,
DCO and CodeRabbit requirements. It removes only `creation` and `update` rules,
which otherwise restrict normal main updates to bypass actors. It adds the
existing language checks, keeps the branch-current requirement, and enables
conversation resolution. It grants no bypass access.

```bash
gh api --paginate --slurp "repos/$dsx_repo/commits/main/check-runs?per_page=100" \
  | jq '[.[].check_runs[]]' > "$dsx_rules_dir/main-checks.json"

cat > "$dsx_rules_dir/required-ci-names.json" <<'JSON'
[
  "SPDX Source Headers",
  "Python Static check",
  "Go Lint",
  "Go Buf Lint",
  "Go Unit Test",
  "TypeScript Lint",
  "TypeScript Unit Test",
  "Python Unit Test (recovery-libs)",
  "Python Unit Test (job-manager)",
  "Python Unit Test (watcher)",
  "Python Unit Test (anomaly-detection-isolator)",
  "Python Unit Test (notifications)",
  "Python Integration Test (recovery-libs)",
  "Python Integration Test (notifications)"
]
JSON

# Resolve names to the app that actually emitted them; reject missing checks.
jq -e --slurpfile names "$dsx_rules_dir/required-ci-names.json" '
  [.[] | select(.app.slug == "github-actions") |
    {context: .name, integration_id: .app.id}] | unique_by(.context) as $seen |
  [$names[0][] as $name | $seen[] | select(.context == $name)] as $required |
  if ($required | length) != ($names[0] | length)
  then error("Required CI names missing from current main; inspect current PR checks")
  else $required end
' "$dsx_rules_dir/main-checks.json" > "$dsx_rules_dir/required-ci.json"

jq -e --slurpfile ci "$dsx_rules_dir/required-ci.json" '
  if ([.rules[] | select(.type == "pull_request")] | length) != 1 or
     ([.rules[] | select(.type == "required_status_checks")] | length) != 1
  then error("Expected exactly one PR rule and one status-check rule")
  else . end |
  {name, target, enforcement, bypass_actors, conditions, rules} |
  .enforcement = "active" |
  .rules |= map(select(.type != "creation" and .type != "update")) |
  .rules |= map(
    if .type == "pull_request" then
      .parameters.required_approving_review_count =
        ([.parameters.required_approving_review_count, 1] | max) |
      .parameters.require_code_owner_review = true |
      .parameters.dismiss_stale_reviews_on_push = true |
      .parameters.require_last_push_approval = true |
      .parameters.required_review_thread_resolution = true
    elif .type == "required_status_checks" then
      .parameters.strict_required_status_checks_policy = true |
      .parameters.required_status_checks =
        ((.parameters.required_status_checks + $ci[0]) |
          unique_by([.context, .integration_id]))
    else . end)
' "$dsx_rules_dir/main.before.json" > "$dsx_rules_dir/main.proposed.json"

jq . "$dsx_rules_dir/main.proposed.json"
```

Before applying, verify those names on a current PR as well. Keep existing
CodeRabbit as required unless you deliberately choose otherwise; its success
is independent of CI. Do not add future docs/aggregate checks until they exist.
The review E2E lane is informational today and is not included as a mandatory
check here. Its public qualification policy changes in the planned PRs.

## 3. Apply main protection, then verify

This block writes the reviewed payload. The comparison detects changes made
since the backup; a mismatch stops the command sequence. GitHub provides no
ruleset dry-run endpoint or atomic compare-and-swap in this recipe, so coordinate
with other repository administrators while applying it.

```bash
gh api "repos/$dsx_repo/rulesets/$dsx_main_id" \
  | jq -S -f "$dsx_rules_dir/writable.jq" > "$dsx_rules_dir/main.latest.json"
cmp "$dsx_rules_dir/main.restore.json" "$dsx_rules_dir/main.latest.json"

gh api --method PUT "repos/$dsx_repo/rulesets/$dsx_main_id" \
  --input "$dsx_rules_dir/main.proposed.json" > "$dsx_rules_dir/main.applied.json"

gh api "repos/$dsx_repo/rulesets/$dsx_main_id" \
  --jq '{id,name,enforcement,bypass_actors,rules}'
gh api "repos/$dsx_repo/rules/branches/main" --paginate \
  --jq '.[] | {type,parameters}'
```

The first response must show `active`; the effective branch rules must include
PR review and the expected status checks. Check a current PR's mergeability
and check results without trying a forbidden push or merging a test PR.

## 4. Prepare and apply copied-PR branch protection

First confirm in repository Settings → Integrations → GitHub Apps that the
installed bot is the `copy-pr-bot` app resolved above. A successful public app
lookup alone does not prove installation on this repository. Confirm that the
bot owns all creation, force-update and deletion operations on the copied
branches. The bot's SHA/trust configuration remains a separate requirement.

This rule deliberately allows the bot to force-update and delete its temporary
branches. Its bypass applies only to this ruleset; it receives no main bypass.
Use the existing ruleset if one was added since this review, rather than creating
a duplicate. Inspect any differently named ruleset covering the same branches
before proceeding.

```bash
gh api --paginate --slurp "repos/$dsx_repo/rulesets?per_page=100" \
  | jq 'add' > "$dsx_rules_dir/rulesets.latest.json"
jq '.[] | select(.target == "branch") | {id,name,source_type,enforcement}' \
  "$dsx_rules_dir/rulesets.latest.json"

dsx_bot_ruleset_id=$(jq -er '
  [.[] | select(.source_type == "Repository" and .name == "Copied PR branches")] |
  if length == 0 then ""
  elif length == 1 then .[0].id | tostring
  else error("Multiple matching rulesets; inspect before changing") end
' "$dsx_rules_dir/rulesets.latest.json")
printf '%s\n' "$dsx_bot_ruleset_id" > "$dsx_rules_dir/bot.original-id"

if [ -n "$dsx_bot_ruleset_id" ]; then
  gh api "repos/$dsx_repo/rulesets/$dsx_bot_ruleset_id" \
    > "$dsx_rules_dir/bot.before.json"
  jq -S -f "$dsx_rules_dir/writable.jq" "$dsx_rules_dir/bot.before.json" \
    > "$dsx_rules_dir/bot.restore.json"
  # Only reuse an existing rule with this exact intended scope and policy.
  jq -e --argjson app "$dsx_bot_app_id" '
    .target == "branch" and
    .conditions.ref_name == {include:["refs/heads/pull-request/*"],exclude:[]} and
    .bypass_actors == [{actor_id:$app,actor_type:"Integration",bypass_mode:"always"}] and
    ([.rules[].type] | sort) == ["creation","deletion","non_fast_forward","update"]
  ' "$dsx_rules_dir/bot.before.json"
  jq '.enforcement = "active"' "$dsx_rules_dir/bot.restore.json" \
    > "$dsx_rules_dir/bot.proposed.json"
else
  jq -n --argjson app "$dsx_bot_app_id" '{
    name: "Copied PR branches", target: "branch", enforcement: "active",
    bypass_actors: [{actor_id:$app,actor_type:"Integration",bypass_mode:"always"}],
    conditions: {ref_name:{include:["refs/heads/pull-request/*"],exclude:[]}},
    rules: [{type:"creation"},{type:"update"},{type:"deletion"},{type:"non_fast_forward"}]
  }' > "$dsx_rules_dir/bot.proposed.json"
fi
jq . "$dsx_rules_dir/bot.proposed.json"
```

After reviewing the payload, run this **apply** block:

```bash
if [ -n "$dsx_bot_ruleset_id" ]; then
  gh api "repos/$dsx_repo/rulesets/$dsx_bot_ruleset_id" \
    | jq -S -f "$dsx_rules_dir/writable.jq" > "$dsx_rules_dir/bot.latest.json"
  cmp "$dsx_rules_dir/bot.restore.json" "$dsx_rules_dir/bot.latest.json"
  gh api --method PUT "repos/$dsx_repo/rulesets/$dsx_bot_ruleset_id" \
    --input "$dsx_rules_dir/bot.proposed.json" > "$dsx_rules_dir/bot.applied.json"
else
  gh api --method POST "repos/$dsx_repo/rulesets" \
    --input "$dsx_rules_dir/bot.proposed.json" > "$dsx_rules_dir/bot.applied.json"
  dsx_bot_ruleset_id=$(jq -er .id "$dsx_rules_dir/bot.applied.json")
fi
printf '%s\n' "$dsx_bot_ruleset_id" > "$dsx_rules_dir/bot.applied-id"
gh api "repos/$dsx_repo/rulesets/$dsx_bot_ruleset_id" \
  --jq '{id,name,enforcement,conditions,bypass_actors,rules}'
```

## 5. Verify release-tag protection

First verify the existing release-tag restrictions and maintainer creation
bypass:

```bash
gh api "repos/$dsx_repo/rulesets/$dsx_tag_id" > "$dsx_rules_dir/tags.current.json"
jq -e --argjson team "$dsx_team_id" '
  .enforcement == "active" and .target == "tag" and
  .conditions.ref_name == {include:["refs/tags/v*"],exclude:[]} and
  (.bypass_actors | length == 1) and
  any(.bypass_actors[]; .actor_type == "Team" and .actor_id == $team and
    .bypass_mode == "always") and
  ([.rules[].type] | sort) == ["creation","deletion","non_fast_forward","update"]
' "$dsx_rules_dir/tags.current.json"
```

If it fails, inspect the difference before changing anything. Do not create a
release tag for this test: tag creation triggers the release workflow. The
separate `helm-release` environment still needs its reviewer/ref restrictions.

The existing team bypass also permits moving or deleting release tags. Before
publication, add a separate `v*` ruleset that prevents updates and deletion for
all actors, including maintainers. Rulesets apply cumulatively: creation stays
under the original rule, while this additional rule has no bypass. The workflow
also resolves the remote tag to its commit after approval and refuses a mismatch
before publication. That check does not replace immutable tags: an authorized
tag update could otherwise race the publication steps.

Prepare the additional rule and check for an existing one of the same name:

```bash
gh api "repos/$dsx_repo/rulesets" --paginate \
  --jq '.[] | select(.name == "Immutable release tags") | .id' \
  > "$dsx_rules_dir/immutable-tags.ids"
test "$(wc -l < "$dsx_rules_dir/immutable-tags.ids")" -le 1
dsx_immutable_tag_id=$(cat "$dsx_rules_dir/immutable-tags.ids")
jq -n '{
  name: "Immutable release tags", target: "tag", enforcement: "active",
  bypass_actors: [],
  conditions: {ref_name: {include: ["refs/tags/v*"], exclude: []}},
  rules: [{type: "update"}, {type: "deletion"}]
}' > "$dsx_rules_dir/immutable-tags.proposed.json"
jq . "$dsx_rules_dir/immutable-tags.proposed.json"
```

If an existing ID was found, inspect that rule and proceed to verification.
Otherwise, review the payload and run this **apply** block:

```bash
test ! -s "$dsx_rules_dir/immutable-tags.ids"
gh api --method POST "repos/$dsx_repo/rulesets" \
  --input "$dsx_rules_dir/immutable-tags.proposed.json" \
  > "$dsx_rules_dir/immutable-tags.applied.json"
dsx_immutable_tag_id=$(jq -er .id "$dsx_rules_dir/immutable-tags.applied.json")
printf '%s\n' "$dsx_immutable_tag_id" > "$dsx_rules_dir/immutable-tags.created-id"
```

Verify the selected existing or newly created rule:

```bash
test -n "$dsx_immutable_tag_id"
gh api "repos/$dsx_repo/rulesets/$dsx_immutable_tag_id" \
  | jq -e '.target == "tag" and .enforcement == "active" and
    .bypass_actors == [] and
    .conditions.ref_name == {include:["refs/tags/v*"],exclude:[]} and
    ([.rules[].type] | sort) == ["deletion","update"]'
```

Do not disable these rules during a release. Repository administrators retain
the ability to change policy; reviewer approval must confirm it remains active.

## Recovery commands

Run only the recovery appropriate to the changed rule, after checking nobody
has made a subsequent intentional update. Restoration returns the saved
configuration, including main's original evaluate state; it is a rollback,
not the desired steady state.

Restore main from this session's backup:

```bash
gh api --method PUT "repos/$dsx_repo/rulesets/$dsx_main_id" \
  --input "$dsx_rules_dir/main.restore.json"
```

Remove only the additional immutable-tag rule created in this session if rollback
is necessary, after stopping any pending release and checking for later changes:

```bash
if [ -s "$dsx_rules_dir/immutable-tags.created-id" ]; then
  dsx_immutable_tag_id=$(cat "$dsx_rules_dir/immutable-tags.created-id")
  gh api --method DELETE "repos/$dsx_repo/rulesets/$dsx_immutable_tag_id"
fi
```

Restore a pre-existing copied-PR ruleset, or delete only the one this session
created. Do not run this block unless the bot apply block completed:

```bash
dsx_bot_ruleset_id=$(cat "$dsx_rules_dir/bot.applied-id")
if [ -s "$dsx_rules_dir/bot.restore.json" ]; then
  gh api --method PUT "repos/$dsx_repo/rulesets/$dsx_bot_ruleset_id" \
    --input "$dsx_rules_dir/bot.restore.json"
else
  gh api --method DELETE "repos/$dsx_repo/rulesets/$dsx_bot_ruleset_id"
fi
```

## Documentation and validation

The commands use GitHub's [repository rules API](https://docs.github.com/en/rest/repos/rules)
and [`gh api --input`](https://cli.github.com/manual/gh_api). Bypass and merge
semantics are documented in the [ruleset reference](https://docs.github.com/en/repositories/configuring-branches-and-merges-in-your-repository/managing-rulesets/available-rules-for-rulesets).

Validation for this runbook: live read-only rule/app/team lookup, Bash syntax
checks, and local JSON transformation/assertions against the fetched ruleset.
No PUT/POST/DELETE call or live bot approval test was executed. The stored
2026-09-09 review snapshot remains historical; the commands obtain fresh backups
when you run them.

## Add checks emitted by the implementation stack

After the reviewed stack runs, add `Helm Lint`,
`Python Unit Test (attribution-adapter)` and
`Fern documentation check and render` to the required CI name list above, then
regenerate and review the main payload using fresh check-run evidence. Bind them
to the actual GitHub Actions app as the runbook does. Do not select a missing
name before its workflow emits it. `Release Packaging` now runs on every main
and copied-PR push; add it after a successful observed run. Tagged publication
also runs packaging plus exact-source code/docs CI and E2E gates directly.

Configure `attribution-qualification` for main-only maintainer-approved private
qualification and `docs-publish` for main-only approved docs publishing, with
self-review disabled. Confirm effective bot failure handling and secret/runner
scope as documented in [CI approval](../governance/ci-approval.md). No environment,
bot deployment, roster test or repository setting was changed by implementation.
