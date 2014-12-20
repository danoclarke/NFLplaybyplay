import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.log4j.Logger;

public class PlayByPlayMapper extends Mapper<LongWritable, Text, Text, Text> {
	Logger logger = Logger.getLogger(PlayByPlayMapper.class);

	private static final char OUTPUT_SEPARATOR = '\t';

	Pattern incompletePass = Pattern
			.compile("([A-Za-z]*\\.?\\s?[A-Za-z]*)\\s*pass.*incomplete.*(to ([A-Za-z]*\\.?\\s?[A-Za-z]*))?");

	Pattern interception = Pattern
			.compile("([A-Za-z]*\\.\\s?[A-Za-z]*).*intended for.*INTERCEPTED by ([A-Za-z]*\\.?\\s?[A-Za-z]*)");

	Pattern completePass = Pattern
			.compile("([A-Za-z]*\\.?\\s?[A-Za-z]*)\\s*pass.*to ([A-Z]*\\.\\s?[A-Za-z]*).*\\(?([A-Z]*\\.\\s?[A-Za-z]*)?\\)?\\s?\\[?([A-Z]*\\.\\s?[A-Za-z]*)?\\]?");

	Pattern punt = Pattern
			.compile("([A-Z]*\\.\\s?[A-Za-z]*)\\s*punts.*to.*\\.\\s?([A-Z]*\\.\\s?[A-Za-z]*)?");

	Pattern run = Pattern
			.compile("([A-Za-z]*\\.?\\s?[A-Za-z]*)\\s*.*[to|for].*\\(?([A-Z]*\\.\\s?[A-Za-z]*)?\\)?\\s?\\[?([A-Z]*\\.\\s?[A-Za-z]*)?\\]?");

	Pattern kickoff = Pattern
			.compile("([A-Z]*\\.\\s?[A-Za-z]*)\\s*kicks.*from.*\\.?\\s?([A-Z]*\\.\\s?[A-Za-z]*)?");

	Pattern spike = Pattern
			.compile("([A-Za-z]*\\.?\\s?[A-Za-z]*)\\s*spiked the ball");

	Pattern fieldGoal = Pattern
			.compile("([A-Za-z]*\\.\\s?[A-Za-z]*)\\s*(\\d*).*field goal");

	Pattern extraPoint = Pattern
			.compile("([A-Za-z]*\\.?\\s?[A-Za-z]*).*extra point");

	Pattern penalty = Pattern.compile(".*PENALTY.*");

	Pattern fumble = Pattern.compile(".*FUMBLES.*");

	Pattern touchdown = Pattern.compile(".*TOUCHDOWN.*");

	Pattern sack = Pattern
			.compile("([A-Za-z]*\\.?\\s?[A-Za-z]*)\\s*.*sacked.*\\(?([A-Z]*\\.\\s?[A-Za-z]*)\\)?\\s?\\[?([A-Z]*\\.\\s?[A-Za-z]*)?\\]?");

	Pattern kneel = Pattern.compile("([A-Za-z]*\\.?\\s?[A-Za-z]*)\\s*kneels");

	Pattern review = Pattern.compile("play under review");

	Pattern scramble = Pattern
			.compile("([A-Za-z]*\\.?\\s?[A-Za-z]*)\\s*scrambles");

	Pattern endQuarter = Pattern.compile("END [QUARTER|GAME]");

	Pattern gameString = Pattern.compile("(\\d*)_([A-Z]*)@([A-Z]*)");

	Pattern[] allPatterns = { incompletePass, interception, completePass, punt,
			kickoff, spike, fieldGoal, extraPoint, sack, kneel, review,
			scramble, endQuarter, run };

	String idPrefix = null;
	int id = 0;

	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String line = value.toString();

		String[] pieces = line.split(",", -1);

		if (pieces.length == 0) {
			// Skip lines that are only commas
			// ,,,,,,,,,,,,
			return;
		}

		String qb = "", offensivePlayer = "", defensivePlayer1 = "", defensivePlayer2 = "";
		boolean hasPenalty = false, hasTouchdown = false, hasFumble = false, hasIncomplete = false, isGoalGood = false;
		String playType = "";
		String kickDistance = "";

		boolean found = false;

		int piecesIndex = -1;

		String playDesc = null;

		// Sometimes the play description is in a different field
		if (pieces[9].length() > 7) {
			playDesc = pieces[9];
			piecesIndex = 9;
		} else if (pieces[11].length() > 7) {
			playDesc = pieces[11];
			piecesIndex = 11;
		} else {
			logger.warn("Line is null \"" + line + "\"");
			return;
		}

		if (pieces.length < piecesIndex + 2) {
			logger.warn("Line is not big enough \"" + line + "\"");
			return;
		}

		for (Pattern pattern : allPatterns) {
			Matcher matcher = pattern.matcher(playDesc);

			if (matcher.find()) {
				found = true;

				if (pattern == incompletePass) {
					qb = matcher.group(1);
					offensivePlayer = matcher.group(3);
					hasIncomplete = true;
					playType = "PASS";
				} else if (pattern == interception) {
					qb = matcher.group(1);
					defensivePlayer1 = matcher.group(2);
					playType = "INTERCEPTION";
				} else if (pattern == completePass) {
					qb = matcher.group(1);
					offensivePlayer = matcher.group(2);
					defensivePlayer1 = matcher.group(3);
					defensivePlayer2 = matcher.group(4);
					playType = "PASS";
				} else if (pattern == punt) {
					qb = matcher.group(1);
					defensivePlayer1 = matcher.group(2);
					playType = "PUNT";
				} else if (pattern == kickoff) {
					offensivePlayer = matcher.group(1);
					defensivePlayer1 = matcher.group(2);
					playType = "KICKOFF";
				} else if (pattern == spike) {
					qb = matcher.group(1);
					playType = "SPIKE";
				} else if (pattern == fieldGoal) {
					qb = matcher.group(1);
					isGoalGood = playDesc.toLowerCase().indexOf("no good") == -1
							&& playDesc.toLowerCase().indexOf("missed") == -1
							&& playDesc.toLowerCase().indexOf("blocked") == -1;
					kickDistance = matcher.group(2);
					playType = "FIELDGOAL";
				} else if (pattern == extraPoint) {
					qb = matcher.group(1);
					isGoalGood = playDesc.toLowerCase().indexOf("no good") == -1
							&& playDesc.toLowerCase().indexOf("missed") == -1
							&& playDesc.toLowerCase().indexOf("blocked") == -1;
					playType = "EXTRAPOINT";
				} else if (pattern == sack) {
					offensivePlayer = matcher.group(1);
					defensivePlayer1 = matcher.group(2);
					defensivePlayer2 = matcher.group(3);

					// Workaround regex bug
					if (defensivePlayer2 != null
							&& defensivePlayer2.equals(".")) {
						defensivePlayer2 = "";
					}

					playType = "SACK";
				} else if (pattern == kneel) {
					qb = matcher.group(1);
					playType = "KNEEL";
				} else if (pattern == review) {
					playType = "REVIEW";
				} else if (pattern == scramble) {
					qb = matcher.group(1);
					playType = "SCRAMBLE";
				} else if (pattern == endQuarter) {
					playType = "END";
				} else if (pattern == run) {
					offensivePlayer = matcher.group(1);
					defensivePlayer1 = matcher.group(2);
					defensivePlayer2 = matcher.group(3);

					// Workaround regex bug
					if (defensivePlayer2 != null
							&& defensivePlayer2.equals(".")) {
						defensivePlayer2 = "";
					}

					playType = "RUN";
				}

				break;
			}
		}

		// Always check for penalties and fumbles and touchdowns
		Matcher matcher = penalty.matcher(playDesc);

		if (matcher.find()) {
			hasPenalty = true;
		}

		matcher = fumble.matcher(playDesc);

		if (matcher.find()) {
			hasFumble = true;
		}

		matcher = touchdown.matcher(playDesc);

		if(matcher.find()) {
			hasTouchdown = true;
		}

		if (found == false) {
			context.getCounter("inc", "notfound").increment(1);
			logger.warn("Did not match \"" + line + "\"");

			return;
		}

		StringBuilder output = new StringBuilder();

		// Process the game output
		Matcher gameMatcher = gameString.matcher(pieces[0]);

		// Process the game output
		if (gameMatcher.find()) {
			// Check that offense and defense is filled in
			if (pieces[4].trim().length() == 0) {
				pieces[4] = gameMatcher.group(3).equals(pieces[5]) ? gameMatcher
						.group(2) : gameMatcher.group(3);
				logger.warn("Replacing offense to be " + pieces[4] + " Off:"
						+ pieces[5]);
			}

			if (pieces[5].trim().length() == 0) {
				pieces[4] = gameMatcher.group(3).equals(pieces[4]) ? gameMatcher
						.group(3) : gameMatcher.group(2);
				logger.warn("Replacing offense to be " + pieces[5] + " Def:"
						+ pieces[4]);
			}
		} else {
			context.getCounter("inc", "gamenotfound").increment(1);
			logger.warn("Game did not match \"" + line + "\"");
			return;
		}

		// Add all of the pieces
		for (int i = 0; i < pieces.length; i++) {
			// Normalize output across all seasons by removing extra info
			if (piecesIndex == 11) {
				if (i == 9 || i == 10 || i == 12 || i == 13 || i == 14) {
					continue;
				}

				output.append(pieces[i]).append(OUTPUT_SEPARATOR);
			} else {
				output.append(pieces[i]).append(OUTPUT_SEPARATOR);
			}
		}

		// Check that extracted data isn't from missing groups
		if (qb == null) {
			qb = "";
		}

		if (offensivePlayer == null) {
			offensivePlayer = "";
		}

		if (defensivePlayer1 == null) {
			defensivePlayer1 = "";
		}

		if (defensivePlayer2 == null) {
			defensivePlayer2 = "";
		}

		// Process the play by play data
		output.append(qb.replaceAll("\\s","")).append(OUTPUT_SEPARATOR);
		output.append(offensivePlayer.replaceAll("\\s","")).append(OUTPUT_SEPARATOR);
		output.append(defensivePlayer1.replaceAll("\\s","")).append(OUTPUT_SEPARATOR);
		output.append(defensivePlayer2.replaceAll("\\s","")).append(OUTPUT_SEPARATOR);
		output.append(hasPenalty).append(OUTPUT_SEPARATOR);
		output.append(hasFumble).append(OUTPUT_SEPARATOR);
		output.append(hasIncomplete).append(OUTPUT_SEPARATOR);
		output.append(isGoalGood).append(OUTPUT_SEPARATOR);
		output.append(playType).append(OUTPUT_SEPARATOR);

		// Process the game output
		output.append(gameMatcher.group(3)).append(OUTPUT_SEPARATOR);
		output.append(gameMatcher.group(2)).append(OUTPUT_SEPARATOR);
		output.append(gameMatcher.group(1)).append(OUTPUT_SEPARATOR);
		
		// Output the unique id of the play
		output.append(idPrefix).append("_").append(StringUtils.leftPad(String.valueOf(id), 8, "0")).append(OUTPUT_SEPARATOR);
		id++;

		//append TD info
		output.append(hasTouchdown).append(OUTPUT_SEPARATOR);

		//append KickDistance Info
		output.append(kickDistance);
		
		context.write(new Text(pieces[0]), new Text(output.toString()));
	}

	@Override
	public void setup(Context context) {
		id = 0;

		FileSplit fileSplit = (FileSplit) context.getInputSplit();
		idPrefix = fileSplit.getPath().getName();
		
		// Crop until the first underscore
		idPrefix = idPrefix.substring(0, idPrefix.indexOf("_"));
	}
}