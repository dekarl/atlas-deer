AtlasOwlStage:
  - ChangesContent
  - ChangesTopics
  - EquivAssert

AtlasOwlStage consumers:
  - from AtlasOwlStage
    - EquivUpdater <- ChangesContent

AtlasDeerStage:
  - ChangesContent
  - ChangesTopics
  - ChangesSchedule
  - ChangesEquivalenceContentGraph

AtlasDeerStage consumers:
  - from AtlasOwlStage:
    - Bootstrap <- ChangesContent
    - Bootstrap <- ChangesTopic
    - EquivGraphUpdate <- EquivAssert
  - from AtlasDeerStage:
    - Indexer <- ChangesContent
    - Indexer <- ChangesTopic
    - EquivalentContentStoreContent <- ChangesContent
    - EquivalentContentStoreGraphs <- ChangesEquivalenceContentGraph
    - EquivalentScheduleStoreSchedule <- ChangesSchedule
    - EquivalentScheduleStoreGraphs <- ChangesEquivalenceContentGraph
