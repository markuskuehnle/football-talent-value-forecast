# Football Talent Forecasting: 1-Year Market Value Prediction

---

> Template:

```markdown
## 1. **Executive Summary**

A one-paragraph TL;DR for busy, non-technical readers.

- Problem → Approach → Outcome
- Include outcome metrics and business value
- Tailor this to your audience (PMs, hiring managers, tech leads)

## 2. **Business Problem & Objective**

- Why does this matter?
- Who benefits from solving it?
- What would success look like?

[1-2 sentences on the real-world problem]

### Value Delivered
[Key metrics or outcomes improved]

## 3. **Data & Methodology**

This is where you describe how you solved the problem.

- Walk through your process using **CRISP-DM**
- If your project includes ML, explain your pipeline using the **FTI Architecture**
- Link to repo structure, code logic, tools used, and key decisions

### Key Technologies
[List of main technologies used]

## 4. **Results & Business Insights**

This isn’t about model accuracy. It’s about business impact.

- What did you discover or predict?
- How would a stakeholder act on this?
- Use visuals (charts, dashboards) but explain what they mean

## 5. **Conclusion & Next Steps**

Show maturity and critical thinking.

- What are the key takeaways?
- What are the limitations of your current approach?
- What would you improve, automate, or deploy?
```

---

## Dev Setup

Python Env:

### 1. Clone or pull the repo
git clone https://github.com/markuskuehnle/football-talent-value-forecast.git
cd football-talent-value-forecast

### 2. Create & activate the same-named venv
uv venv .football-talent-env
source .football-talent-env/bin/activate

### 3. Install everything listed in pyproject.toml
uv pip install -e .        # reads dependencies = [...] and installs them

### 4. Install Pre-Commit Hooks

on CLI run:

```bash
pre-commit install
pre-commit run --all-files
```
