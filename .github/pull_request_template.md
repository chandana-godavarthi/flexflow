### Describe Your Changes
<!-- Explain what you have done & tag your assigned user story !-->

Changes related to (AB#ticket_number)
<!-- Mandatory to provide ado work item in format AB#ticket_number-->

### Type of change
<!-- You should try to choose 1 option and uncheck options that aren't relevant -->
- [x] User Story implementation (feat)
- [ ] Bug fix (fix)
- [ ] Documentation (docs)
- [ ] Refactor (refactor)
- [ ] Style (style)
- [ ] Test (test)
- [ ] Grunt (grunt)

### Success Evidence
[Link](https://yoururl) to successful runs of your job in dev.
Paste 1 or more screenshots of the SparkUI during your job run showing CPU, Memory, and worker utilization.

### Breaking Change?
<!-- 
#### Examples of breaking changes
- Schema changes on a bronze/silver/gold table
- Calculation/transformation changes
- View logic changes
-->

<!-- You should select yes/no -->
## Does your change have the potential to break something downstream?

- [ ] Yes
- [x] No

### Checklist
<!-- add an x in [] if done, don't mark items that you didn't do !-->
- [ ] I have read the [**Code Quality**](https://developerportal.pg.com/docs/default/Component/composite-pipelines/style-guides/python_pyspark_style_guide/) document, and my changes adheres to the DPC standard
- [ ] I have commented on my code, particularly in hard-to-understand areas.
- [ ] I have added tests (DQ, unit, etc.) that prove my fix is effective or that my feature works.
- [ ] I have selected a cluster configuration which is optimized for my workload (node type, spot instances, photon, auto scaling considered)
- [ ] All new and existing tests passed.
