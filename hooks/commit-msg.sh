#!/usr/bin/env bash
#
# This pre-commit hook validates the commit message to conform to the following template: [TICKET-NUMBER] Description

# Regex to validate in commit msg:
# ^                 Start of line
# \[                Match against a square bracket.
# (                 Open 1st capture group:
# ([A-Z,0-9]{3,})   2nd capture group: Match against a alphanumeric string (min. 3 chars)
# (-[0-9]{3,})?     3rd capture group: Match against hyphen and a numeric value (min. 3 chars). Optional.
# )                 End 1st capture group.
# ((                Open 4th & 5th capture group:
# \,\s              Match against a comma and a white space.
# (                 Open 6th capture group:
# [A-Z,0-9]{3,}     Same validation as 2nd capture group.
# (-[0-9]{3,})?     Same validation as 3rd capture group.
# ))                End 5th and 6th capture group.
# ?                 Make entire 5th capture group optional.
# )                 End 4th capture group.
# +                 Allow an infinite repeat of the 4th capture group: allowing for multiple ticket numbers.
# \]                Match against a square bracket.
# \s                Match against a white space.
# \w{1,}            Match against at an infinite number of words, but at least 1 word.


# Set variables

    STATUS=1 # default state is reject, only pass if valid regex match.

    REGEX="^\[(([A-Z,0-9]{3,})(-[0-9]{3,})?)((\, ([A-Z,0-9]{3,}(-[0-9]{3,})?))?)+\] .{1,}"

# Error response:
err="\n COMMIT REJECTED: Invaild commit message. \n"
err="$err \n # REQUIRED: Must start with a square bracket. '['"
err="$err \n # REQUIRED: Must be followed by a capitalized alphanumeric string (min. 3 chars), the prefix of a JIRA/ALM ticket number. (FCA, TFM etc.)"
err="$err \n ## OPTIONAL: Allows for a hyphen."
err="$err \n ## OPTIONAL: Allows for number (min. 3 chars), the numeric component of a JIRA/ALM ticket number."
err="$err \n ## OPTIONAL: Allows for an infinite number of ticket codes, that are separated by a comma and space: [TFM-241, FCA-3124, ..etc.]"
err="$err \n # REQUIRED: Must be followed with a spuare bracket ']'"
err="$err \n # REQUIRED: Must be followed by a white space."
err="$err \n # REQUIRED: Must be followed by a description with at least one character.\n"
err="$err \n # VALID  COMMIT MESSAGE: [TFM-002, FCA-003] & sample commit message that passes all validation \n"

# Capture commit message
MESSAGE=$(cat "$1")

# If we are committing a merge conflict, then accept the default merge message.
if [[ -f "`git rev-parse --git-dir`/MERGE_HEAD" && $MESSAGE =~ Merge\ .* ]]; then
    exit 0
fi

# Run regex validation
# STATUS: 0 --> Pass, 1 --> Failed
(echo "$MESSAGE" | grep -Eq "$REGEX") && STATUS=0 || STATUS=1

# Exit commit-msg appropriately
if [ "$STATUS" == 1 ]; then
    printf "$err"
fi

exit $STATUS