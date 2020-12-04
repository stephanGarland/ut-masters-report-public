SELECT
  u1.login AS actor_login,
  prc.user_id AS actor_id,
  prc.comment_id AS comment_id,
  prc.body AS comment,
  p.name AS repo,
  p.language AS language,
  u2.login AS author_login,
  c.author_id AS author_id,
  prc.pull_request_id AS pr_id,
  prc.commit_id AS c_id,
  c.created_at AS commit_date
FROM
  `ghtorrent-290615.project_commits.pr_comments_small` prc
JOIN
  `ghtorrent-290615.project_commits.project_commits_small` pc
ON
  pc.commit_id = prc.commit_id
JOIN
  `ghtorrent-290615.project_commits.commits_small` c
ON
  pc.idproject_id = c.project_id
JOIN
  `ghtorrentmysql1906.MySQL1906.projects` p
ON
  c.project_id = p.id
JOIN 
  `ghtorrentmysql1906.MySQL1906.pull_request_history` prh
ON
  prc.pull_request_id = prh.pull_request_id
JOIN
  `ghtorrentmysql1906.MySQL1906.users` u1
ON
  prc.user_id = u1.id
JOIN
  `ghtorrentmysql1906.MySQL1906.users` u2
ON
  c.author_id = u2.id
WHERE 
  p.deleted = 0
AND
  p.forked_from IS NULL
AND
  prh.action = "opened"
AND
  c.created_at BETWEEN "$DATE_START" AND "$DATE_END"
;