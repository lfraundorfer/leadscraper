# HOW_TO

This guide explains how to use the app as it works today.

It is intentionally about operating the CRM, not about API keys, SMTP setup, or environment configuration.

## Daily Workflow

If you are using the app day to day, this is the shortest practical flow.

### Daily workflow for an existing campaign

1. Pick the correct active campaign in the sidebar.
2. Open `Dashboard` and check:
   - total leads
   - draft stale
   - research stale
   - today's actions
3. If you changed messaging, flyer, examples, or portfolio assets, go to `Campaigns`, save the campaign changes, then regenerate drafts with `Analyze` or `Generate All Pending`.
4. If you need fresh lead research, run `Research`.
5. Open `Review Queue` and approve the drafts you want to use now.
6. Open `Outreach` and send or log the chosen first channel.
7. Use `All Leads` for notes, price changes, filtering, and one-off draft regeneration.

### Daily workflow for a brand-new campaign

1. Create and activate the campaign.
2. Run `Scrape`.
3. Run `Migrate`.
4. Run `Enrich`.
5. Run `Research`.
6. Run `Analyze`.
7. Review and approve drafts in `Review Queue`.
8. Send or log contact attempts in `Outreach`.

### Safe operator rules

- Always check the active campaign in the sidebar before doing anything.
- Treat `Migrate` carefully on older campaigns because it can currently renumber IDs if new raw rows were appended.
- If drafts are marked stale, regenerate before sending.
- If research is marked stale, rerun `Research` before trusting rank and competitor data.

## What This App Does

The app is a campaign-based lead CRM for local outreach.

Each campaign has its own:

- keyword and location
- lead CSV
- flyer
- portfolio images
- campaign wording and offer text
- research and draft history

Examples:

- legacy campaign: `Installateur Wien`
- new campaign: `Schluesseldienst Wien`

All pages in the app work on the **active campaign only**.

## Main Concepts

### Active campaign

The active campaign is selected in the sidebar.

Everything you do after that uses that campaign's files:

- Campaigns
- Dashboard
- Review Queue
- Outreach
- All Leads

### Where files live

- Legacy `Installateur Wien` uses `new_leads.csv`
- New campaigns use `campaigns/<campaign_id>/leads.csv`
- Campaign config lives in `campaigns/<campaign_id>/config.json`
- Campaign flyer lives in `campaigns/<campaign_id>/assets/`
- Campaign portfolio images live in `campaigns/<campaign_id>/portfolio/`

### Lead lifecycle

Typical lead flow:

`new -> draft_ready -> approved -> contacted -> replied / meeting_scheduled / won / lost / no_contact / blacklist`

### Fresh vs stale

The app tracks whether research or drafts are outdated for the current campaign config.

- `Draft stale`: the campaign wording/assets changed after the draft was generated
- `Research stale`: the keyword/location/rank setup changed after research was generated

When something is stale, regenerate it before using it.

## Starting The App

If the project is already configured, start the UI and use the sidebar to move between pages.

Typical command:

```bash
.venv/bin/streamlit run app.py
```

## Page-By-Page Usage

## Campaigns

This is where you switch campaigns, create new ones, update campaign-specific settings, and run the pipeline for the active campaign.

### Switching campaigns

Use the sidebar selectbox to switch the active campaign.

What happens:

- the active campaign changes immediately
- all pages now use that campaign's files and leads
- Dashboard, Review Queue, Outreach, and All Leads all refresh into that campaign context

The `Active Campaign` box on the page is display only. The actual switcher is in the sidebar.

### Creating campaigns

Use `Create and Activate` when you want to start a new niche/location combination.

Enter:

- `Keyword`
- `Location`

Then click `Create and Activate`.

What happens:

- If the campaign does not exist yet, the app creates a campaign folder and config
- If it already exists, the app just activates it
- It becomes the active campaign immediately
- It does **not** create leads yet
- It does **not** overwrite `new_leads.csv` unless the active campaign itself is the legacy installateur campaign

For a brand-new campaign like `Schluesseldienst Wien`, the app creates:

- `campaigns/schluesseldienst_wien/config.json`
- `campaigns/schluesseldienst_wien/assets/`
- `campaigns/schluesseldienst_wien/portfolio/`

The lead CSV is created later, usually on first `Scrape`.

### Updating campaigns

Once a campaign is active, use the rest of the page to update it.

#### Active Campaign box

This shows:

- label
- CSV path
- ID prefix
- config version

Use it as a quick check that you are working in the right campaign before pressing any stage buttons.

#### Run pipeline buttons

These buttons always operate on the **currently active campaign**.

##### Scrape

Use this to pull raw leads from Herold for the active campaign keyword and location.

What it does:

- writes to the active campaign CSV
- creates the CSV if it does not exist yet
- appends new raw rows
- skips duplicates by normalized company name

What it does not do:

- it does not wipe the file first
- it does not assign CRM IDs
- it does not create drafts

Safe expectation:

- new campaign: creates `campaigns/<id>/leads.csv`
- existing campaign: adds new raw leads to the existing CSV

##### Migrate

Use this after scraping raw leads into a campaign CSV.

What it does:

- adds CRM columns
- assigns IDs like `INSTWIEN-0001` or `SCHLWIEN-0001`
- fills default CRM fields like `Status`, `Contact_Count`, and draft/research version fields where possible

Important current behavior:

- if every row already has the active campaign prefix, `Migrate` does nothing
- if the CSV is mixed, meaning some rows are already migrated and some new raw rows were appended, `Migrate` currently reassigns IDs across the whole file from top to bottom

Practical rule:

- treat `Migrate` carefully on older campaigns
- on a fresh campaign, it is the normal next step after `Scrape`

##### Enrich

Use this to fill missing contact names from `FirmenABC_Link`.

What it does:

- targets leads with a FirmenABC link
- skips leads already marked as enriched unless forced elsewhere
- fills `Kontaktname` if found
- writes `Enriched_At`
- saves as it goes

This is optional but useful before review and outreach.

##### Research

Use this to gather website and Google research for each lead.

What it does:

- categorizes the website
- fetches and analyzes website content
- fetches Google rating / review count
- checks rank for the active campaign keyword
- stores competitors and research metadata
- clears `Research_Stale`

It only targets leads that are missing research or marked as stale.

##### Analyze

Use this to generate the outreach drafts.

What it does:

- scores the website
- determines pain points
- chooses a first channel
- assigns priority
- generates:
  - email draft
  - WhatsApp draft
  - hook category in `Template_Used`
- clears `Draft_Stale`
- usually sets the lead to `draft_ready`

This is the step that puts leads into the Review Queue.

#### Save Campaign Config

Use this to edit campaign-specific wording and commercial settings.

Examples:

- label
- keyword
- location
- service wording
- sender name/company/site/phone/email
- pricing
- example intro
- offer summary
- portfolio URLs

What happens after save:

- config version increases
- draft version increases when meaningful campaign text/settings changed
- research version also increases when `keyword`, `location`, or `rank keyword template` changed

Practical effect:

- copy and asset changes make old drafts stale
- keyword/rank changes make both research and drafts stale

Important:

- changing keyword or location here does not rename the campaign folder
- it also does not move the existing CSV to a new folder

#### Save Flyer

Uploads the flyer image for the active campaign.

What happens:

- the file is written into the campaign flyer path
- the campaign draft version is bumped
- existing drafts become stale

#### Save Portfolio Images

Uploads portfolio reference images for the active campaign.

What happens:

- files are written into the campaign portfolio folder
- the campaign draft version is bumped
- existing drafts become stale

## Dashboard

This is the summary page for the active campaign.

It shows:

- total leads
- draft-ready leads
- approved fresh leads
- draft stale count
- research stale count
- today's actions
- pipeline counts by status

### Generate All Pending

This button appears when there are researched leads that need drafts or stale leads that need new drafts.

Use it to bulk-generate drafts without going lead by lead.

It is most useful after:

- `Research`
- campaign text changes
- flyer/portfolio changes

## Review Queue

This is where you check generated drafts before sending anything.

Only leads with:

- `Status = draft_ready`
- `Draft_Stale != 1`

appear here.

For each lead you can:

- inspect research
- view pain points
- see portfolio references
- edit the email subject and body
- edit the WhatsApp draft
- choose the preferred first channel

### Approve

What it does:

- saves your edited drafts
- stores the chosen channel
- sets `Status = approved`
- sets `Drafts_Approved = 1`

After that, the lead moves to `Outreach`.

### Skip

What it does:

- hides the lead from the Review Queue for the current app session
- does not change the CSV
- does not delete the draft

Use `Restore Skipped` to bring skipped leads back into the queue.

### Blacklist

What it does:

- sets `Status = blacklist`
- removes it from normal follow-up flow

## Outreach

This page shows approved, fresh leads ready for contact.

Only leads with:

- `Status = approved`
- `Drafts_Approved = 1`
- `Draft_Stale != 1`

appear here.

The page only shows the lead's currently selected channel.

That means one lead may contain all three draft types, but Outreach focuses on the chosen first channel.

### Email leads

Available actions:

- `Send Email`
- `Mark as sent`

`Send Email`:

- sends the email via SMTP
- includes the flyer inline if one exists
- logs the contact on success

`Mark as sent`:

- does not send anything
- only logs the contact attempt

### WhatsApp leads

Available actions:

- `Open WhatsApp`
- `Mark as sent`

`Open WhatsApp`:

- opens a `wa.me` link with the draft prefilled
- does not log the send by itself

`Mark as sent`:

- logs the WhatsApp outreach

### Phone leads

Available actions:

- `Called`
- `Voicemail`
- `No answer`

Each one logs the call result and schedules the next follow-up when appropriate.

## All Leads

This is the campaign-wide detail page.

Use it to:

- search
- filter
- review lead details
- edit per-lead price
- edit notes
- blacklist leads
- regenerate drafts for individual leads

### Search

Current search matches:

- company
- address
- contact name
- email

Current limitation:

- it does not search by ID, phone, or notes yet

### Filters

You can filter by:

- status
- channel
- priority
- freshness
- page size

Filters apply when you click `Apply Filters`.

### Save

The `Save` button on a lead stores:

- `Price`
- `Notes`

If you change the price and want the draft text to reflect it, regenerate drafts afterward.

### Generate Drafts

Use this to regenerate drafts for one lead.

This is useful when:

- the lead is draft-stale
- you changed campaign messaging
- you updated pricing and want the copy refreshed

## Follow-Ups And Scheduling

When an outreach action is logged, the app updates the lead automatically.

Typical results:

- first outreach attempt -> `Status = contacted`
- next follow-up date is scheduled
- next follow-up channel is chosen automatically

Current follow-up timing:

- after first touch: 3 days
- after second touch: 4 days
- after third touch: 7 days

If a lead has been contacted 3+ times and still has no reply, it can be auto-archived as `no_contact` after 14 days from the last contact date.

## Recommended Workflows

### New campaign

Use this order:

1. Create and activate the campaign
2. Scrape
3. Migrate
4. Enrich
5. Research
6. Analyze
7. Review Queue: approve the good drafts
8. Outreach: send or log contact attempts

### Existing campaign you want to continue

Use this order:

1. Switch to the campaign in the sidebar
2. Check Dashboard counts
3. If you need more leads: Scrape
4. If new raw rows were appended: Migrate carefully
5. If research is stale or missing: Research
6. If drafts are stale or missing: Analyze or Generate All Pending
7. Review Queue
8. Outreach

### Existing campaign with only messaging changes

Use this order:

1. Save Campaign Config and/or upload new flyer/portfolio
2. Check stale counts
3. Run Analyze or Generate All Pending
4. Review Queue
5. Outreach

## Important Current Caveats

These are worth knowing before you rely on the workflow heavily.

### 1. `Migrate` can renumber a campaign

If you scrape new raw rows into an already migrated CSV, then run `Migrate`, IDs may be reassigned across the whole campaign file.

### 2. `Create and Activate` does not create leads immediately

A new campaign starts with config and asset folders only. The campaign CSV usually appears on first `Scrape`.

### 3. Search in `All Leads` is limited

It currently searches only:

- company
- address
- contact
- email

So searching by `INSTWIEN-0000`, phone number, or notes will not work yet.

### 4. Actual send honors the active campaign sender email

Current behavior:

- if the active campaign has `sender_email`, outbound email uses it as `From` and `Reply-To`
- otherwise it falls back to the global `.env` `SENDER_EMAIL`
- SMTP authentication still uses `SMTP_USER` / `SMTP_PASS`, so some providers may expect that mailbox to match the sender domain

This keeps the visible sender and the actual outbound sender aligned more reliably across campaigns.

## Short Practical Cheat Sheet

If you just want the shortest safe way to operate the app:

1. Pick the right active campaign in the sidebar.
2. On `Campaigns`, run `Scrape -> Migrate -> Enrich -> Research -> Analyze` for a new campaign.
3. On `Review Queue`, approve only the drafts you actually want to use first.
4. On `Outreach`, send or log the chosen first channel.
5. On `All Leads`, edit notes/prices and regenerate drafts when needed.
6. If you changed flyer, offer text, keyword, or examples, expect stale items and regenerate before sending.
