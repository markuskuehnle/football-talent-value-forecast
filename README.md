# Football Talent Forecasting: 1-Year Market Value Prediction

---

> Template:

```
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

## Setup 

### Setup virtual environment with UV

```bash
uv venv .venv
```

activate your venv, then continue with the next step

### Install Pre-Commit Hooks

on CLI run:

```bash
pre-commit install
pre-commit run --all-files
```