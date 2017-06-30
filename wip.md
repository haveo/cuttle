whenIsDef
========



[a0; b0[ .... [an; bn[

[a0; b0[


------------

REFACTOR

- remove completed backfills
- atomic update of state
- cache filtered job intervalmaps

------------

next
====

- get Todos
- get Running => intervalset of jobs with running child
- get Done => intervalset of satisifed dependencies
- toRun = (Todos and satisifiedDeps) \ childRunning
- slotsIncluded(toRun)

IntervalSet

- from AscList
- intersection
- filter IntervalMap by IntervalSet (== intersection)
intersect :: f a -> f b -> f a


- disjoint: lo >= hi (assuming order)
- leftOf: lo < lo
- rightOf: hi > hi

UNSAFE (assume order)

In job:
A: grid

IntervalMap / IntervalSet
- slots included: map first to next, iterate until previous(snd) then .grouping
- slots intersecting: map first to previous, iterate until next(snd) (doublons -> Set ?)

- lo < lo
- hi <= hi
- lo < hi
- lo <= hi

intersect unbounded intervalmap with bounded interval

IntervalMap (always defined?)
- ignore
- done
- todo (backfill?)
- running (execution?)

operations on IntervalMap:
- change domain to discrete (grouping)
- only closedOpen

scheduler (next)
  get just the todo -> Option[Backfill]
  get done and running and compute dependencies
  filter todo with intervalset
  extract periods to run (maxPeriods?) (.grouped)

IntervalSet to Set (discrete)

time <-> slots:
- (interval of) slot to interval of time
- interval of time to interval of slot (only inside?)

