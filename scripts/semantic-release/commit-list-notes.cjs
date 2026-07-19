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

function normalizeRepositoryUrl(repositoryUrl) {
  if (!repositoryUrl) {
    return null;
  }

  let url = repositoryUrl.trim().replace(/\.git$/, '');

  const sshMatch = url.match(/^git@([^:]+):(.+)$/);
  if (sshMatch) {
    return `https://${sshMatch[1]}/${sshMatch[2]}`;
  }

  const sshProtocolMatch = url.match(/^ssh:\/\/git@([^/]+)\/(.+)$/);
  if (sshProtocolMatch) {
    return `https://${sshProtocolMatch[1]}/${sshProtocolMatch[2]}`;
  }

  return url;
}

function formatReleaseHeader(context) {
  const version = context.nextRelease?.version;
  if (!version) {
    return null;
  }

  const date = new Date().toISOString().slice(0, 10);
  const repo = normalizeRepositoryUrl(context.repositoryUrl);
  const nextTag = context.nextRelease.gitTag || `v${version}`;
  const lastTag = context.lastRelease?.gitTag;

  if (repo && lastTag) {
    return `## [${version}](${repo}/compare/${lastTag}...${nextTag}) (${date})`;
  }
  if (repo) {
    return `## [${version}](${repo}/releases/tag/${nextTag}) (${date})`;
  }
  return `## ${version} (${date})`;
}

function withReleaseHeader(body, context) {
  const header = formatReleaseHeader(context);
  if (!header) {
    return body;
  }
  return [header, '', body].join('\n');
}

async function generateNotes(_pluginConfig, context) {
  const fromTag = context.lastRelease?.gitTag;
  const commits = listCommits(fromTag);

  if (commits.length === 0) {
    return withReleaseHeader('No user-facing commits in this release.', context);
  }

  const body = ['## Commits', '', ...commits.map((commit) => `- ${commit}`)].join('\n');
  return withReleaseHeader(body, context);
}

module.exports = {
  generateNotes,
  listCommits,
  RELEASE_COMMIT_RE,
  normalizeRepositoryUrl,
  formatReleaseHeader,
};
