# Decisions from the first team meeting, 2 October 2026

Present: Barnabas Juhasz, Basak Ocak, Felix Wegener, Emmanuel Oyelana, Lucia Ivan.

1. Version control is GitHub only. No GitLab for now. A UZH GitLab mirror can be added later if the supervisor asks for it.
2. New repository, clean slate. The August 2026 prototype repository is archived. Reuse from it is recorded in docs/mvp_lineage.md and stated in the report.
3. Tasks, features and bugs are GitHub issues on one project board. Documents, sheets, user study signups, competitor analysis and the idea board live in the OpenKnowledge internal folder on the team Google Drive.
4. Team Google Workspace at openknowledgehub.app, one account per member, used for external communication and sign ups. The domain is openknowledgehub.app because openknowledge was taken; rebranding stays possible.
5. Cloud is the GCP project owned by Barnabas, with Google for Startups credits through the ETH Agentic Systems Lab. Infrastructure is written as OpenTofu in a separate private repository (issue #1), this repository holds only a pointer to it.
6. Backend is Python with FastAPI. The frontend is rebuilt from scratch.
7. AI coding tools are allowed. Their use is declared per component in docs/ai_usage.md and per pull request, at the level of component and purpose.
8. The weekly meeting is on Fridays, in person. Room booking rotates because one person can book four hours. The team registers at the ETH Student Project House for a dedicated space.
9. Supervision. The ETH lab has no capacity for weekly meetings. Barnabas is at the lab on Wednesdays and Thursdays and relays questions, urgent items go to Robert or Kevin. Grading is by Prof. Tessone at UZH. The team keeps him informed, latest at the alpha, with a visit to the BDLT lab during the pilot phase.
10. Final deliverables are a report written as a paper plus a presentation. The report is to be published.
11. Registration. Prof. Tessone sent the registration form to Daniela Bernstein, who handles master projects at UZH. Written confirmation follows, work starts now.
12. Barnabas gives a walkthrough session of the prototype so everyone starts from the same picture.
