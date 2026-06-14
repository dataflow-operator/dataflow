const RELEASE_COMMIT_RE = /^chore\(release\):/i;

function listCommits(fromTag) {
  const args = ['log', '--pretty=format:%h %s', '--no-merges', '--reverse'];
  if (fromTag) {
    args.push(`${fromTag}..HEAD`);
  }

  let output;
  try {
    output = require('child_process')
      .execFileSync('git', args, { encoding: 'utf-8' })
      .trim();
  } catch {
    return [];
  }

  if (!output) {
    return [];
  }

  return output
    .split('\n')
    .filter((line) => {
      const message = line.replace(/^[0-9a-f]+\s+/, '');
      return message.length > 0 && !RELEASE_COMMIT_RE.test(message);
    });
}

async function generateNotes(_pluginConfig, context) {
  const fromTag = context.lastRelease?.gitTag;
  const commits = listCommits(fromTag);

  if (commits.length === 0) {
    return 'No user-facing commits in this release.';
  }

  return ['## Commits', '', ...commits.map((commit) => `- ${commit}`)].join('\n');
}

module.exports = { generateNotes, listCommits, RELEASE_COMMIT_RE };
