
public class test {
	public static void main(String[] args) {
		char[] input = new char[17];
		String s = "Mr John Smith    ";
		for (int i = 0; i < s.length(); i++) {
			input[i] = s.charAt(i);
		}
		input = replace(input, 13);
		for (int i = 0; i < input.length; i++) {
			System.out.print(input[i]);
		}
	}
	public static char[] replace(char[] str, int truelength) {
		char[] ans = new char[str.length];
		int index = 0;
		for (int i = 0; i < truelength; i++) {
			if (str[i] == ' ') {
				ans[index] = '%';
				ans[index+1] = '2';
				ans[index+2] = '0';
				index += 3;
			} else {
				ans[index] = str[i];
				index++;
			}
		}
		return ans;
	}
	
}
