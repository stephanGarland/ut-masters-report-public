SELECT DISTINCT
    cm.actor_login,
    ug1.gender AS actor_gender,
    cm.actor_id,
    cm.repo,
    cm.language,
    cm.comment,
    ml.label AS sentiment,
    cm.author_login,
    ug2.gender AS author_gender,
    cm.author_id,
    cm.comment_id,
    cm.pr_id,
    cm.c_id AS commit_id,
    cm.commit_date
FROM `ghtorrent-290615.project_commits.combined_$month` cm
JOIN
    `ghtorrent -290615.project_commits.$month_load` ml
ON
  cm.comment_id = ml.comment_id
JOIN
    `ghtorrent -290615.project_commits.users_gendered_high_prob` ug1
ON
  cm.actor_login = ug1.login
JOIN
  `ghtorrent-290615.project_commits.users_gendered_high_prob` ug2
ON
  cm.author_login = ug2.login
;